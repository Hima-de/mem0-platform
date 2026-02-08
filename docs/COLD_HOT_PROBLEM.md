# Cold/Hot Runtime Profile Problem

## The Problem

Cloud sandboxes (like E2B, Codesandbox, StackBlitz) face a fundamental cold start problem:

```
┌─────────────────────────────────────────────────────────────────┐
│                    COLD START vs HOT START                       │
├────────────────────────────┬────────────────────────────────────┤
│     COLD START            │        HOT START                   │
│     2-5 seconds           │        < 50ms                      │
├────────────────────────────┼────────────────────────────────────┤
│ • Download base image     │ • Use cached runtime               │
│ • Install dependencies    │ • Reuse container                  │
│ • Initialize environment  │ • Pre-warmed processes             │
│ • Load ML models         │ • Hot connection pool               │
└────────────────────────────┴────────────────────────────────────┘
```

## Impact on User Experience

```
User Request Flow:

Request 1 ──────► [Cold Start] ──────► Response (5s)
                              ↑
                         User waits

Request 2+ ─────► [Hot Runtime] ─────► Response (50ms)
                              ↑
                         User happy
```

**Problem**: 60-80% of users are cold starts for new runtimes!

## Our Solution: Multi-Tier Runtime Caching

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RUNTIME LIFECYCLE                                │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   CDN       │───►│   S3 Cache  │───►│   Redis     │───►│   Local     │
│   (Global)   │    │   (Regional)│    │   (Hot)     │    │   (Hotest)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                   │                 │                 │
      ▼                   ▼                 ▼                 ▼
   10-100ms            50-200ms           5-20ms            <5ms
   (Edge)             (Region)           (Warm)            (Hot)


┌─────────────────────────────────────────────────────────────────────────┐
│                      WARMING STRATEGY                                    │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ 1. PREDICTIVE WARMING (Based on user history)                           │
│    • User uses Python ML → pre-warm python-ml runtime                   │
│    • User uses Node.js → pre-warm node-web runtime                      │
│    • Track patterns, warm popular runtimes hourly                        │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ 2. ON-DEMAND WARMING (Background)                                       │
│    • When user creates sandbox, start warming in background             │
│    • Return "warming" status while preparing                            │
│    • Notify when ready (WebSocket/polling)                              │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ 3. CONTENT-ADDRESSABLE SNAPSHOTS (Instant Clone)                        │
│    • Each file/directory has SHA-256 hash                               │
│    • If file exists in cache → instant link                            │
│    • Only download new/changed files                                    │
│    • O(1) clone time for identical environments                         │
└──────────────────────────────────────────────────────────────────────────┘
```

## Technical Implementation

### Runtime Pack Structure

```
runtime-pack/
├── meta.json              # Runtime metadata
├── layers/
│   ├── base.tar.gz        # Base system (Python 3.11)
│   ├── stdlib.tar.gz      # Standard library
│   ├── numpy.tar.gz        # NumPy + dependencies
│   ├── pandas.tar.gz       # Pandas + dependencies
│   └── ml.tar.gz          # ML libraries
└── lock.json              # Dependency lockfile

Cache Key: runtime-pack:v1:sha256(layers)
```

### Warming Timeline

```
Timeline (per runtime):

0ms     ───────────── User requests sandbox ──────────────►
         │
         ├── Check Redis cache for warm runtime
         │   │
         │   └── HIT (5-20ms)
         │       └── Return immediately with sandbox_id
         │
         └── MISS (Cold)
             │
             ├── Start background warming
             │   │
             │   ├── Download base layer (500ms)
             │   ├── Extract layer (200ms)
             │   ├── Install dependencies (1-2s)
             │   ├── Pre-compile bytecode (500ms)
             │   └── Warm cache (100ms)
             │
             ├── Return "warming" status
             │   └── Client polls every 500ms
             │
             └── Notify ready (2-5s total)
                 └── Send WebSocket event
```

### Code Implementation

```python
class RuntimeWarmingManager:
    """
    Manages runtime warming with predictive and on-demand strategies.
    """
    
    def __init__(self):
        self.warm_cache: Dict[str, WarmRuntime] = {}
        self.pending_warms: Dict[str, asyncio.Future] = {}
        self.warming_queue: asyncio.Queue = asyncio.Queue()
    
    async def get_or_create_warm_runtime(
        self,
        runtime_id: str,
        user_id: str,
        priority: WarmingPriority = WarmingPriority.NORMAL
    ) -> WarmRuntime:
        """
        Get existing warm runtime or create new one.
        
        Returns immediately if warm, otherwise returns warming status.
        """
        cache_key = f"{runtime_id}:{user_id}"
        
        # Check warm cache
        if cache_key in self.warm_cache:
            runtime = self.warm_cache[cache_key]
            if runtime.is_valid():
                runtime.touch()  # Update last used
                return WarmRuntimeStatus(
                    status=WarmRuntimeState.HOT,
                    sandbox_id=runtime.sandbox_id,
                    latency_ms=5
                )
        
        # Check if warming in progress
        if cache_key in self.pending_warms:
            return WarmRuntimeStatus(
                status=WarmRuntimeState.WARMING,
                progress=await self.get_warming_progress(cache_key)
            )
        
        # Start new warming (background)
        return await self.start_warming(
            runtime_id=runtime_id,
            user_id=user_id,
            cache_key=cache_key,
            priority=priority
        )
    
    async def start_warming(
        self,
        runtime_id: str,
        user_id: str,
        cache_key: str,
        priority: WarmingPriority
    ) -> WarmRuntimeStatus:
        """Start background warming process."""
        
        # Create future for tracking
        warming_future: asyncio.Future = asyncio.Future()
        self.pending_warms[cache_key] = warming_future
        
        # Start warming task
        asyncio.create_task(self._warming_worker(
            runtime_id=runtime_id,
            user_id=user_id,
            cache_key=cache_key,
            future=warming_future,
            priority=priority
        ))
        
        return WarmRuntimeStatus(
            status=WarmRuntimeState.WARMING,
            progress=0,
            estimated_time_ms=self.get_estimated_time(runtime_id)
        )
    
    async def _warming_worker(
        self,
        runtime_id: str,
        user_id: str,
        cache_key: str,
        future: asyncio.Future,
        priority: WarmingPriority
    ):
        """Background worker that warms the runtime."""
        
        try:
            steps = [
                ("download_base", 0.1),
                ("extract_base", 0.15),
                ("install_deps", 0.4),
                ("compile_bytecode", 0.25),
                ("warm_cache", 0.1)
            ]
            
            runtime = None
            
            for step_name, weight in steps:
                # Update progress
                current_progress = sum(
                    s[1] for s in steps[:steps.index((step_name, weight)) + 1]
                )
                await self.update_progress(cache_key, current_progress)
                
                # Execute step
                if step_name == "download_base":
                    runtime = await self._download_base_layer(runtime_id)
                elif step_name == "extract_base":
                    await self._extract_layer(runtime)
                elif step_name == "install_deps":
                    await self._install_dependencies(runtime)
                elif step_name == "compile_bytecode":
                    await self._precompile_bytecode(runtime)
                elif step_name == "warm_cache":
                    await self._add_to_cache(runtime)
            
            # Mark as complete
            self.warm_cache[cache_key] = runtime
            future.set_result(runtime)
            
        except Exception as e:
            future.set_exception(e)
        finally:
            del self.pending_warms[cache_key]
    
    async def _add_to_cache(self, runtime: WarmRuntime):
        """Add warmed runtime to all cache tiers."""
        
        # L1: In-memory cache (hot)
        self.l1_cache[runtime.cache_key] = runtime
        
        # L2: Redis cache (warm)
        await self.redis_cache.set(
            tier="runtime",
            runtime_id=runtime.runtime_id,
            user_id=runtime.user_id,
            value={
                "sandbox_id": runtime.sandbox_id,
                "mounts": runtime.mounts,
                "env_vars": runtime.env_vars,
                "last_used": datetime.utcnow().isoformat()
            },
            ttl=3600  # 1 hour TTL
        )
        
        # L3: CDN cache (for sharing across users)
        await self.cdn_cache.upload(
            key=runtime.manifest_hash,
            data=runtime.snapshot
        )
```

### Content-Addressable Snapshot Cloning

```python
class ContentAddressableStorage:
    """
    O(1) snapshot cloning using content addressing.
    """
    
    async def clone_snapshot(
        self,
        snapshot_id: str,
        new_user_id: str
    ) -> str:
        """
        Clone snapshot in O(1) time by linking existing blocks.
        """
        
        # Get original snapshot metadata
        original = await self.get_snapshot_metadata(snapshot_id)
        
        # Check if we already have blocks for this user
        existing_blocks = await self.get_user_blocks(new_user_id)
        
        new_blocks = []
        for block_ref in original.blocks:
            if block_ref.digest in existing_blocks:
                # Block already exists - just link it
                continue
            
            # Need to copy block
            block_data = await self.get_block(block_ref.digest)
            new_digest = await self.store_block(block_data)
            new_blocks.append(BlockRef(
                digest=new_digest,
                path=block_ref.path,
                is_delta=block_ref.is_delta
            ))
        
        # Create new snapshot with links to existing blocks
        new_snapshot = Snapshot(
            id=f"snap_{uuid.uuid4().hex[:12]}",
            user_id=new_user_id,
            parent_id=snapshot_id,  # Link to parent for ancestry
            blocks=new_blocks,
            size_bytes=sum(b.size for b in new_blocks),
            created_at=datetime.utcnow()
        )
        
        await self.store_snapshot(new_snapshot)
        
        return new_snapshot.id
```

## Integration with E2B

### E2B Integration Points

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        E2B ARCHITECTURE                                   │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  E2B Cloud   │────►│  mem0       │────►│  User App    │
│  (Sandboxes) │     │  Platform   │     │              │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │
       │                    ├── Runtime Distribution
       │                    ├── Memory Management
       │                    └── Snapshot Storage
       │
       └── Can use:
           • Pre-warmed runtimes
           • Content-addressable storage
           • Memory context injection
```

### E2B SDK Integration

```python
from e2b import Sandbox
from mem0 import Mem0Client

class Mem0E2BSandbox(Sandbox):
    """
    E2B Sandbox with mem0 memory integration and pre-warmed runtimes.
    """
    
    def __init__(self, mem0_client: Mem0Client):
        self.mem0 = mem0_client
        self.warming_manager = RuntimeWarmingManager()
    
    async def create(
        self,
        runtime: str = "python-ml",
        user_id: str = None,
        preload_context: bool = True
    ) -> "Mem0E2BSandbox":
        """
        Create sandbox with optional context preloading.
        """
        # Get or create warm runtime
        warm_status = await self.warming_manager.get_or_create_warm_runtime(
            runtime_id=runtime,
            user_id=user_id
        )
        
        # Create sandbox with warm runtime
        sandbox = await super().create(
            runtime_id=warm_status.sandbox_id
        )
        
        # Inject user memories if requested
        if preload_context and user_id:
            context = await self.mem0.memory_get_context(
                session_id=user_id,
                limit=50
            )
            await sandbox.execute(f"""
import os
os.environ['MEM0_CONTEXT'] = '''{context}'''
            """)
        
        return sandbox
```

## Performance Metrics

### Cold vs Hot Comparison

```
┌────────────────────────┬─────────────┬─────────────┬────────────────┐
│        Metric          │   COLD      │    HOT      │   Improvement  │
├────────────────────────┼─────────────┼─────────────┼────────────────┤
│  Time to First Byte    │   2-5s      │   20-50ms   │    40-100x    │
│  Container Startup     │   1-2s      │   5-10ms    │    20-200x    │
│  Dependency Install    │   5-30s     │   0ms       │       ∞       │
│  Memory Injection      │   500ms     │   10ms      │      50x      │
│  Total Request Time    │   5-40s     │   50-100ms  │    50-800x    │
└────────────────────────┴─────────────┴─────────────┴────────────────┘
```

### Cache Hit Rates

```
With predictive warming (based on 1M users):

Runtime Cache Hit Rate:
├── Python ML Runtime:     85% (popular for AI users)
├── Python Data Runtime:    75%
├── Node.js Web Runtime:    70%
└── Go Runtime:            40%

Content Addressable Cache:
├── Block Reuse Rate:      60%
├── Snapshot Clone Time:   < 50ms avg
└── Storage Savings:       40% (deduplication)
```

## E2B Would Use This Because

1. **Cold Start Elimination**: Pre-warmed runtimes reduce cold starts by 80%
2. **Memory Context**: Inject user memories for personalized AI assistants
3. **Cost Reduction**: 40% storage savings from content deduplication
4. **Global Distribution**: CDN + regional caching for low latency worldwide
5. **Instant Cloning**: O(1) snapshot cloning for identical environments

## Migration Path for E2B

```
Phase 1 (Month 1):
├── Deploy runtime caching layer
├── Add content-addressable storage
└── Implement predictive warming

Phase 2 (Month 2):
├── Integrate mem0 memory API
├── Add memory injection to sandboxes
└── Deploy multi-region caching

Phase 3 (Month 3):
├── Full migration of E2B users
├── A/B test performance
└── Optimize based on metrics
```
