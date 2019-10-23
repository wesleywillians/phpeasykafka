<?php
declare(strict_types=1);


namespace PHPEasykafka;

interface KafkaProducerAwareInterface
{
    public function produce($payload, $key = null);
}