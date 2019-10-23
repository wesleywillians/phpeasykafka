<?php
declare(strict_types=1);

namespace PHPEasykafka;

class KafkaProducer implements KafkaProducerAwareInterface
{

    private $brokers;
    private $topicName;
    private $topic;
    private $configs;
    private $debugMode;
    private $producer;

    /**
     * KafkaProducer constructor.
     * @param $brokers
     * @param $topicName
     * @param array $configs
     * @param bool $debugMode
     */
    public function __construct(
        BrokerCollectionAwareInterface $brokers,
        $topicName,
        array $configs,
        bool $debugMode = true
    ) {
        $this->brokers = $brokers;
        $this->topicName = $topicName;
        $this->configs = $configs;
        $this->debugMode = $debugMode;
        $this->setup();
    }


    private function setup(): void
    {
        $this->producer = new \RdKafka\Producer();
        $this->producer->addBrokers($this->brokers->getBrokersList());
        $topicConf = $this->setupTopic();
        $this->topic = $this->producer->newTopic($this->topicName, $topicConf);
    }

    /**
     * @return \RdKafka\TopicConf
     */
    private function setupTopic(): \RdKafka\TopicConf
    {
        $topicConf = new \RdKafka\TopicConf();
        if ($this->configs) {
            foreach ($this->configs['topic'] as $k => $v) {
                $topicConf->set($k, $v);
            }
        }
        return $topicConf;
    }


    public function produce($payload, $key = null): void
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $key);

        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll(1);
        }
//        $this->producer->purge(RD_KAFKA_PURGE_F_QUEUE);
        $this->producer->flush(1000);
    }

    public function getProducer(): \RdKafka\Producer
    {
        return $this->producer;
    }

}
