<?php
declare(strict_types=1);


namespace PHPEasykafka;

use Psr\Container\ContainerInterface;

class KafkaConsumer implements KafkaConsumerAwareInterface
{
    private $brokers;
    private $topics;
    private $groupId;
    private $configs;
    private $consumer;
    private $container;
    private $debugMode;

    /**
     * KafkaProducer constructor.
     * @param BrokerCollectionAwareInterface $brokers
     * @param array $topics
     * @param $groupId
     * @param array $configs
     * @param ContainerInterface $container
     * @param bool $debugMode
     */
    public function __construct(
        BrokerCollectionAwareInterface $brokers,
        array $topics,
        string $groupId,
        array $configs,
        ContainerInterface $container,
        bool $debugMode = true
    ) {
        $this->brokers = $brokers;
        $this->topics = $topics;
        $this->groupId = $groupId;
        $this->configs = $configs;
        $this->container = $container;
        $this->debugMode = $debugMode;
        $this->setup();
    }

    private function setup()
    {
        $conf = $this->setupConf();
        $this->consumer = new \RdKafka\KafkaConsumer($conf);
        $this->consumer->subscribe($this->topics);
    }

    public function consume($ms, array $classesList)
    {
        $loop = true;
        while ($loop) {

            $message = $this->consumer->consume($ms);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    foreach ($classesList as $class) {
                        try {
                            $this->executeHandler($class, $message);
                        } catch (\Exception $e) {
                            if ($this->debugMode) {
                                echo "Error: " . $e->getMessage();
                                echo "Offset: " . $message->offset;
                            }
                            $loop = false;
                            break;
                        }
                    }
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    if ($this->debugMode) {
                        echo "No more messages; will wait for more\n";
                    }
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    if ($this->debugMode) {
                        echo "Timed out\n";
                    }
                    break;

                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }

    /**
     * @return \RdKafka\Conf
     */
    private function setupConf(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    if ($this->debugMode) {
                        echo "Assign: ";
                        var_dump($partitions);
                    }
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    if ($this->debugMode) {
                        echo "Revoke: ";
                        var_dump($partitions);
                    }
                    $kafka->assign(null);
                    break;

                default:
                    throw new \Exception($err);
            }
        });

        $conf->set('group.id', $this->groupId);
        $conf->set('metadata.broker.list', $this->brokers->getBrokersList());

        foreach ($this->configs['consumer'] as $k => $v) {
            $conf->set($k, $v);
        }
        return $conf;
    }

    /**
     * @param $class
     * @param $message
     * @throws \Exception
     */
    private function executeHandler($class, $message): void
    {
        $class = $this->container->get($class);
        if (!$class instanceof KafkaConsumerHandlerInterface) {
            throw new \Exception("The object of " . get_class($class) . " does not implement KafkaConsumerHandlerInterface.");
        }
        $class($message, $this->consumer);
    }
}
