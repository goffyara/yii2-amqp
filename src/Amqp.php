<?php
namespace goffyara\amqp\components;

use yii\base\Component;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Amqp extends Component
{
    public $host;
    public $port;
    public $user;
    public $password;
    public $vhost = '/';

    private $_channel;
    private $_connection;

    public function getConnection() {

        if (is_object($this->_connection)) {
            return $this->_connection;
        }

        $this->_connection = new AMQPStreamConnection(
            $this->host,
            $this->port,
            $this->user,
            $this->password,
            $this->vhost
        );

        return $this->_connection;
    }

    public function getChannel() {

        if (is_object($this->_channel)) {
            return $this->_channel;
        }

        $this->_channel = $this->getConnection()->channel();
        return $this->_channel;
    }

    public function getMessage(string $data , array $properties = []) {

        $defaultProperties = [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
        ];

        $properties = array_merge($defaultProperties, $properties);

        return new AMQPMessage($data, $properties);
    }

    public function publish(AMQPMessage $message, string $key, string $exchange = '')
    {
        $this->channel->basic_publish($message, $exchange, $key);
    }

    public function nack(AMQPMessage $message)
    {
        return $message
            ->delivery_info['channel']
            ->basic_nack($message->delivery_info['delivery_tag']);
    }

    public function ack(AMQPMessage $message)
    {
        return $message
            ->delivery_info['channel']
            ->basic_ack($message->delivery_info['delivery_tag']);
    }

    public function requeue(AMQPMessage $message)
    {
        $message
            ->delivery_info['channel']
            ->basic_nack($message->delivery_info['delivery_tag'], false, true);
    }
}