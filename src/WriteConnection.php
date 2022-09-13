<?php

declare(strict_types=1);

namespace PixelFederation\OpenSwooleZMQ;

use RuntimeException;
use SplQueue;
use UnderflowException;
use ZMQ;
use ZMQSocketException;

abstract class WriteConnection extends Connection
{
    /**
     * @var SplQueue<string|array<string>>
     */
    private SplQueue $messages;

    private bool $connected = false;

    private bool $listening = false;

    protected function __construct(string $dsn, int $type)
    {
        if (!in_array($type, [ZMQ::SOCKET_PUSH, ZMQ::SOCKET_PUB], true)) {
            throw new UnderflowException(
                sprintf('Socket type not supported: %d', $type)
            );
        }

        /** @psalm-suppress MixedPropertyTypeCoercion */
        $this->messages = new SplQueue();

        parent::__construct($dsn, $type);
    }

    public function connect(): void
    {
        if ($this->connected) {
            throw new ZmqProblem("Already connected.");
        }

        $this->getSocket()->connect($this->dsn);
        /** @psalm-suppress InvalidArgument */
        swoole_event_add(
            $this->fd, /** @phpstan-ignore-line */
            function (): void {
                $this->handleReadEvent();
            },
            function (): void {
                $this->handleWriteEvent();
            },
            SWOOLE_EVENT_READ
        );
        $this->connected = true;
    }

    public function send(string $message): void
    {
        $this->doSend($message);
    }

    /**
     * @param array<string> $messages
     */
    public function sendMulti(array $messages): void
    {
        $this->doSend($messages);
    }

    protected function closeSocket(): void
    {
        /** @var array{connect: array<string>} $endpoints */
        $endpoints = $this->getSocket()->getEndpoints();

        if (empty($endpoints['connect'])) {
            return;
        }

        foreach ($endpoints['connect'] as $endpoint) {
            $this->getSocket()->disconnect($endpoint);
        }
    }

    /**
     * @param string|array<string> $message
     */
    private function doSend($message): void
    {
        if ($this->closed) {
            throw new RuntimeException('Connection was already closed.');
        }

        $this->messages->enqueue($message);

        if ($this->listening) {
            return;
        }

        $this->listening = true;

        /**
         * @psalm-suppress InvalidArgument
         * @phpstan-ignore-next-line
         */
        if (!swoole_event_set($this->fd, null, null, SWOOLE_EVENT_READ | SWOOLE_EVENT_WRITE)) {
            throw new RuntimeException('Unable to override event handler.');
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
            $hasOutEvents = $events & ZMQ::POLL_OUT && (int) $this->listening;

            if (!$hasOutEvents) {
                break;
            }

            $this->handleWriteEvent();
        }
    }

    private function handleWriteEvent(): void
    {
        while ($this->messages->count() > 0) {
            $message = $this->messages->dequeue();

            try {
                if (is_array($message)) {
                    $this->getSocket()->sendmulti($message, ZMQ::MODE_DONTWAIT);

                    continue;
                }

                $this->getSocket()->send($message, ZMQ::MODE_DONTWAIT);
            } catch (ZMQSocketException $e) {
                if ($this->onError === null) {
                    return;
                }

                call_user_func($this->onError, $e);
            }
        }

        $this->listening = false;

        /**
         * @psalm-suppress InvalidArgument
         * @phpstan-ignore-next-line
         */
        if (!swoole_event_set($this->fd, null, null, SWOOLE_EVENT_READ)) {
            throw new RuntimeException('Unable to override event handler.');
        }
    }
}