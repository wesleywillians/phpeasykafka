<?php
declare(strict_types=1);


namespace PHPEasykafka;

class BrokerCollection implements BrokerCollectionAwareInterface
{
    private $brokers;

    public function __construct()
    {
        $this->brokers = [];
    }

    public function addBroker(BrokerAwareInterface $broker): void
    {
        $this->brokers[] = $broker;
    }

    public function getBrokersList(): string
    {
        return implode(",", $this->brokers);
    }
}