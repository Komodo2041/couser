<?php

namespace Clue\React\Redis\Io;

use Clue\Redis\Protocol\Factory as ProtocolFactory;
use React\EventLoop\Loop;
use React\Promise\Deferred;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use React\Socket\ConnectionInterface;
use React\Socket\Connector;
use React\Socket\ConnectorInterface;
use function React\Promise\reject;

/**
 * @internal
 */
class Factory
{
    /** @var ConnectorInterface */
    private $connector;

    /** @var ProtocolFactory */
    private $protocol;

    /**
     * @param ?ConnectorInterface $connector
     * @param ?ProtocolFactory $protocol
     */
    public function __construct(?ConnectorInterface $connector = null, ?ProtocolFactory $protocol = null)
    {
        $this->connector = $connector ?: new Connector();
        $this->protocol = $protocol ?: new ProtocolFactory();
    }

    /**
     * Create Redis client connected to address of given redis instance
     *
     * @param string $uri Redis server URI to connect to
     * @return PromiseInterface<StreamingClient> Promise that will
     *     be fulfilled with `StreamingClient` on success or rejects with `\Exception` on error.
     */
    public function createClient(string $uri): PromiseInterface
    {
        // support `redis+unix://` scheme for Unix domain socket (UDS) paths
        if (preg_match('/^(redis\+unix:\/\/(?:[^@]*@)?)(.+)$/', $uri, $match)) {
            $parts = parse_url($match[1] . 'localhost/' . $match[2]);
        } else {
            $parts = parse_url($uri);
        }

        $uri = preg_replace(['/(:)[^:\/]*(@)/', '/([?&]password=).*?($|&)/'], '$1***$2', $uri);
        assert(is_array($parts) && isset($parts['scheme'], $parts['host']));
        assert(in_array($parts['scheme'], ['redis', 'rediss', 'redis+unix']));

        $args = [];
        parse_str($parts['query'] ?? '', $args);

        $authority = $parts['host'] . ':' . ($parts['port'] ?? 6379);
        if ($parts['scheme'] === 'rediss') {
            $authority = 'tls://' . $authority;
        } elseif ($parts['scheme'] === 'redis+unix') {
            assert(isset($parts['path']));
            $authority = 'unix://' . substr($parts['path'], 1);
            unset($parts['path']);
        }

        $connecting = $this->connector->connect($authority);

        $deferred = new Deferred(function ($_, $reject) use ($connecting, $uri) {
            // connection cancelled, start with rejecting attempt, then clean up
            $reject(new \RuntimeException(
                'Connection to ' . $uri . ' cancelled (ECONNABORTED)',
                defined('SOCKET_ECONNABORTED') ? SOCKET_ECONNABORTED : 103
            ));

            // either close successful connection or cancel pending connection attempt
            $connecting->then(function (ConnectionInterface $connection) {
                $connection->close();
            }, function () {
                // ignore to avoid reporting unhandled rejection
            });
            $connecting->cancel();
        });

        $promise = $connecting->then(function (ConnectionInterface $stream) {
            return new StreamingClient($stream, $this->protocol->createResponseParser(), $this->protocol->createSerializer());
        }, function (\Throwable $e) use ($uri) {
            throw new \RuntimeException(
                'Connection to ' . $uri . ' failed: ' . $e->getMessage(),
                $e->getCode(),
                $e
            );
        });

        // use `?password=secret` query or `user:secret@host` password form URL
        if (isset($args['password']) || isset($parts['pass'])) {
            $pass = $args['password'] ?? rawurldecode($parts['pass']); // @phpstan-ignore-line
            \assert(\is_string($pass));
            $promise = $promise->then(function (StreamingClient $redis) use ($pass, $uri) {
                return $redis->callAsync('auth', $pass)->then(
                    function () use ($redis) {
                        return $redis;
                    },
                    function (\Throwable $e) use ($redis, $uri) {
                        $redis->close();

                        $const = '';
                        $errno = $e->getCode();
                        if ($errno === 0) {
                            $const = ' (EACCES)';
                            $errno = $e->getCode() ?: (defined('SOCKET_EACCES') ? SOCKET_EACCES : 13);
                        }

                        throw new \RuntimeException(
                            'Connection to ' . $uri . ' failed during AUTH command: ' . $e->getMessage() . $const,
                            $errno,
                            $e
                        );
                    }
                );
            });
        }

        // use `?db=1` query or `/1` path (skip first slash)
        if (isset($args['db']) || (isset($parts['path']) && $parts['path'] !== '/')) {
            $db = $args['db'] ?? substr($parts['path'], 1); // @phpstan-ignore-line
            \assert(\is_string($db));
            $promise = $promise->then(function (StreamingClient $redis) use ($db, $uri) {
                return $redis->callAsync('select', $db)->then(
                    function () use ($redis) {
                        return $redis;
                    },
                    function (\Throwable $e) use ($redis, $uri) {
                        $redis->close();

                        $const = '';
                        $errno = $e->getCode();
                        if ($errno === 0 && strpos($e->getMessage(), 'NOAUTH ') === 0) {
                            $const = ' (EACCES)';
                            $errno = defined('SOCKET_EACCES') ? SOCKET_EACCES : 13;
                        } elseif ($errno === 0) {
                            $const = ' (ENOENT)';
                            $errno = defined('SOCKET_ENOENT') ? SOCKET_ENOENT : 2;
                        }

                        throw new \RuntimeException(
                            'Connection to ' . $uri . ' failed during SELECT command: ' . $e->getMessage() . $const,
                            $errno,
                            $e
                        );
                    }
                );
            });
        }

        $promise->then([$deferred, 'resolve'], [$deferred, 'reject']);

        // use timeout from explicit ?timeout=x parameter or default to PHP's default_socket_timeout (60)
        $timeout = isset($args['timeout']) ? (float) $args['timeout'] : (int) ini_get("default_socket_timeout");
        if ($timeout < 0) {
            return $deferred->promise();
        }

        $promise = $deferred->promise();

        /** @var Promise<StreamingClient> */
        $ret = new Promise(function (callable $resolve, callable $reject) use ($timeout, $promise, $uri): void {
            /** @var ?\React\EventLoop\TimerInterface */
            $timer = null;
            $promise = $promise->then(function (StreamingClient $v) use (&$timer, $resolve): void {
                if ($timer) {
                    Loop::cancelTimer($timer);
                }
                $timer = false;
                $resolve($v);
            }, function (\Throwable $e) use (&$timer, $reject): void {
                if ($timer) {
                    Loop::cancelTimer($timer);
                }
                $timer = false;
                $reject($e);
            });

            // promise already settled => no need to start timer
            if ($timer === false) {
                return;
            }

            // start timeout timer which will cancel the pending promise
            $timer = Loop::addTimer($timeout, function () use ($timeout, &$promise, $reject, $uri): void {
                $reject(new \RuntimeException(
                    'Connection to ' . $uri . ' timed out after ' . $timeout . ' seconds (ETIMEDOUT)',
                    \defined('SOCKET_ETIMEDOUT') ? \SOCKET_ETIMEDOUT : 110
                ));

                // Cancel pending connection to clean up any underlying resources and references.
                // Avoid garbage references in call stack by passing pending promise by reference.
                \assert($promise instanceof PromiseInterface);
                $promise->cancel();
                $promise = null;
            });
        }, function () use (&$promise): void {
            // Cancelling this promise will cancel the pending connection, thus triggering the rejection logic above.
            // Avoid garbage references in call stack by passing pending promise by reference.
            \assert($promise instanceof PromiseInterface);
            $promise->cancel();
            $promise = null;
        });

        // variable assignment needed for legacy PHPStan on PHP 7.1 only
        return $ret;
    }
}
