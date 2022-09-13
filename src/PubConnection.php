<?php

declare(strict_types=1);

namespace PixelFederation\OpenSwooleZMQ;

use Swoole\Coroutine;
use ZMQ;

final class PubConnection extends WriteConnection
{
    public function __construct(string $dsn)
    {
        parent::__construct($dsn, ZMQ::SOCKET_PUB);
    }

    /**
     * @SuppressWarnings(PHPMD.StaticAccess)
     */
    public function connect(): void
    {
        parent::connect();

        // without this the sub connection socket misses first couple of messages, not sure why
        // @todo investigate further
        for ($i = 0; $i < 2; $i++) {
            $this->send('@@@ IGNORE THIS');
            /** @phpstan-ignore-next-line */
            Coroutine::usleep(1000);
        }
    }
}
