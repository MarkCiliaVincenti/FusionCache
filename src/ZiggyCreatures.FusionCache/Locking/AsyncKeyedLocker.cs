using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using AsyncKeyedLock;
using Microsoft.Extensions.Logging;

namespace ZiggyCreatures.Caching.Fusion.Locking;

/// <summary>
/// An implementation of <see cref="IFusionCacheMemoryLocker"/> based on AsyncKeyedLock.
/// </summary>
internal sealed class AsyncKeyedLocker
	: IFusionCacheMemoryLocker
{
	private readonly AsyncKeyedLocker<object> _locker;

	/// <summary>
	/// Initializes a new instance of the <see cref="AsyncKeyedLocker"/> class.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public AsyncKeyedLocker()
	{
		_locker = new AsyncKeyedLocker<object>(o =>
		{
			o.PoolSize = 20;
			o.PoolInitialFill = 1;
		});
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="AsyncKeyedLocker"/> class.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public AsyncKeyedLocker(Action<AsyncKeyedLockOptions> options)
	{
		_locker = new AsyncKeyedLocker<object>(options);
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="AsyncKeyedLocker"/> class.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public AsyncKeyedLocker(Action<AsyncKeyedLockOptions> options, int concurrencyLevel, int capacity)
	{
		_locker = new AsyncKeyedLocker<object>(options, concurrencyLevel, capacity);
	}

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public async ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string key, string operationId, TimeSpan timeout, ILogger? logger, CancellationToken token)
	{
		var releaser = _locker.GetOrAdd(key);

		if (logger?.IsEnabled(LogLevel.Trace) ?? false)
			logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): waiting to acquire the LOCK", cacheName, cacheInstanceId, operationId, key);

		var acquired = await releaser.SemaphoreSlim.WaitAsync(timeout, token).ConfigureAwait(false);

		if (logger?.IsEnabled(LogLevel.Trace) ?? false)
			logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK acquired", cacheName, cacheInstanceId, operationId, key);

		return (acquired) ? releaser : null;
	}

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public object? AcquireLock(string cacheName, string cacheInstanceId, string key, string operationId, TimeSpan timeout, ILogger? logger, CancellationToken token)
	{
		var releaser = _locker.GetOrAdd(key);

		if (logger?.IsEnabled(LogLevel.Trace) ?? false)
			logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): waiting to acquire the LOCK", cacheName, cacheInstanceId, operationId, key);

		var acquired = releaser.SemaphoreSlim.Wait(timeout, token);

		if (logger?.IsEnabled(LogLevel.Trace) ?? false)
			logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK acquired", cacheName, cacheInstanceId, operationId, key);

		return (acquired) ? releaser : null;
	}

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ReleaseLock(string cacheName, string cacheInstanceId, string key, string operationId, object? lockObj, ILogger? logger)
	{
		if (lockObj is null)
			return;

		((AsyncKeyedLockReleaser<object>)lockObj).Dispose();
	}

	// IDISPOSABLE
	private bool disposedValue;
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Dispose(bool disposing)
	{
		if (!disposedValue)
		{
			if (disposing)
			{
				if (_locker is not null)
				{
					_locker.Dispose();
				}
			}

			disposedValue = true;
		}
	}

	/// <inheritdoc/>
	public void Dispose()
	{
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}
}
