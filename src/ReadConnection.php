<?php

declare(strict_types=1);

namespace PixelFederation\OpenSwooleZMQ;

use Closure;
use UnderflowException;
use ZMQ;

abstract class ReadConnection extends Connection
{
    private Closure $onMessage;

    private bool $listening = false;

    /**
     * @SuppressWarnings(PHPMD.StaticAccess)
     */
    protected function __construct(string $dsn, int $type, callable $onMessage)
    {
        if (!in_array($type, [ZMQ::SOCKET_PULL, ZMQ::SOCKET_SUB], true)) {
            throw new UnderflowException(
                sprintf('Socket type not supported: %d', $type)
            );
        }

        $this->onMessage = Closure::fromCallable($onMessage);

        parent::__construct($dsn, $type);
    }

    public function bind(): void
    {
        if ($this->listening) {
            throw new ZmqProblem("Already listening.");
        }

        $this->getSocket()->bind($this->dsn);

        /** @psalm-suppress InvalidArgument */
        swoole_event_add(
            $this->fd, /** @phpstan-ignore-line */
            function (): void {
                $this->handleReadEvent();
            },
            null, /** @phpstan-ignore-line */
            SWOOLE_EVENT_READ
        );
        $this->listening = true;
    }

    protected function closeSocket(): void
    {
        /** @var array{bind: array<string>} $endpoints */
        $endpoints = $this->getSocket()->getEndpoints();

        if (empty($endpoints['bind'])) {
            return;
        }

        foreach ($endpoints['bind'] as $endpoint) {
            $this->getSocket()->unbind($endpoint);
        }
    }

    private function handleReadEvent(): void
    {
        while (!$this->closed) {
            /**
             * @psalm-suppress InvalidArgument
             * @phpstan-ignore-next-line
             * @var int $events
             */
            $events = $this->getSocket()->getSockOpt(ZMQ::SOCKOPT_EVENTS);
            $hasInEvents = $events & ZMQ::POLL_IN;

            if (!$hasInEvents) {
                break;
            }

            $messages = $this->getSocket()->recvmulti(ZMQ::MODE_DONTWAIT);

            foreach ($messages as $message) {
                call_user_func($this->onMessage, $message);
            }
        }
    }
}
