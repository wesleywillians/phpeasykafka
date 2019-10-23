<?php
declare(strict_types=1);


namespace PHPEasykafka;

interface KafkaConsumerAwareInterface
{
    public function consume($ms, array $classesList);
}