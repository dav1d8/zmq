<?php

declare(strict_types=1);

namespace PixelFederation\OpenSwooleZMQ;

use Closure;
use RuntimeException;
use ZMQ;
use ZMQContext;
use ZMQSocket;

abstract class Connection
{
    protected string $dsn;

    protected bool $closed = false;

    /**
     * @var resource|null
     */
    protected $fd = null;

    protected ?Closure $onError = null;

    private ZMQContext $context;

    private ?ZMQSocket $socket = null;

    private ?Closure $onClose = null;

    private int $type;

    abstract protected function closeSocket(): void;

    protected function __construct(string $dsn, int $type)
    {
        $this->dsn = $dsn;
        $this->type = $type;
        $this->context = new ZMQContext();
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        if ($this->onClose !== null) {
            call_user_func($this->onClose);
        }

        if ($this->fd !== null) {
            /**
             * @psalm-suppress InvalidArgument
             * @phpstan-ignore-next-line
             */
            if (!swoole_event_del($this->fd)) {
                throw new RuntimeException('Unable to remove socket event.');
            }
            unset($this->fd);
        }

        if ($this->socket !== null) {
            $this->closeSocket();
            unset($this->socket);
        }

        $this->closed = true;
    }

    /**
     * @SuppressWarnings(PHPMD.StaticAccess)
     */
    public function setCloseHandler(callable $handler): void
    {
        $this->onClose = Closure::fromCallable($handler);
    }

    /**
     * @SuppressWarnings(PHPMD.StaticAccess)
     */
    public function setErrorHandler(callable $handler): void
    {
        $this->onError = Closure::fromCallable($handler);
    }

    protected function initSocket(): ZMQSocket
    {
        $socket = $this->context->getSocket($this->type);
        // fix for php process hang in terminal if ZMQ handler is initialised
        // @see http://stackoverflow.com/questions/26884319/php-cli-process-hangs-forever-on-exit
        // Without this line, the script will wait forever after the exit statement
        $socket->setSockOpt(ZMQ::SOCKOPT_LINGER, 1000);

        return $socket;
    }

    protected function getSocket(): ZMQSocket
    {
        if ($this->socket === null) {
            $this->socket = $this->initSocket();
            /**
             * @psalm-suppress InvalidPropertyAssignmentValue
             * @psalm-suppress InvalidArgument
             * @phpstan-ignore-next-line
             */
            $this->fd = $this->socket->getSockOpt(ZMQ::SOCKOPT_FD);
        }

        return $this->socket;
    }
}
