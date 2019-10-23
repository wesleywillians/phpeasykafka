<?php
declare(strict_types=1);

namespace PHPEasykafka;


class Broker implements BrokerAwareInterface
{
    private $host;
    private $port;

    /**
     * Broker constructor.
     * @param $host
     * @param $port
     */
    public function __construct(string $host, string $port)
    {
        $this->host = $host;
        $this->port = $port;
    }

    public function __toString(): string
    {
        return $this->host . ":" . $this->port;
    }
}