using System.Runtime.CompilerServices;
using AsyncKeyedLock;
using Microsoft.Extensions.Logging;

namespace ZiggyCreatures.Caching.Fusion.Locking.AsyncKeyed;

/// <summary>
/// An implementation of <see cref="IFusionCacheMemoryLocker"/> based on AsyncKeyedLocker.
/// </summary>
public sealed class AsyncKeyedMemoryLocker
	: IFusionCacheMemoryLocker
{
	private readonly AsyncKeyedLocker<string> _locker;

	/// <summary>
	/// Initializes a new instance of the <see cref="AsyncKeyedLocker"/> class.
	/// </summary>
	public AsyncKeyedMemoryLocker(AsyncKeyedLockOptions? options = null)
	{
		options ??= new AsyncKeyedLockOptions();

		_locker = new AsyncKeyedLocker<string>(options);
	}

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public async ValueTask<object?> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		=> await _locker.LockOrNullAsync(key, timeout, token).ConfigureAwait(false);

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
		=> _locker.LockOrNull(key, timeout, token);

	/// <inheritdoc/>
	public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key, object? lockObj, ILogger? logger)
	{
		if (lockObj is null)
			return;

		((IDisposable)lockObj).Dispose();
	}

	// IDISPOSABLE
	private bool disposedValue;

	/// <inheritdoc/>
	public void Dispose()
	{
		if (disposedValue)
		{
			return;
		}

		_locker?.Dispose();

		disposedValue = true;
	}
}
