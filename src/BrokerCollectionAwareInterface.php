<?php
declare(strict_types=1);


namespace PHPEasykafka;

interface BrokerCollectionAwareInterface
{
    public function addBroker(BrokerAwareInterface $broker): void;

    public function getBrokersList(): string;
}