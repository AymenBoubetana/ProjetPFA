<?php

require 'vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
use Aws\DynamoDb\Exception\DynamoDbException;

$client = new DynamoDbClient([
    'region' => 'eu-north-1', 
    'version' => 'latest',
    'credentials' => [
        'key' => 'Access_key',
        'secret' => 'Access_secret',
    ]
]);

$marshaler = new Marshaler();

$data = [
    "code_synchronisations" => "aymen-001", 
    "age" => 31,
    "name" => "aymen",
    "lastname" => "boubetana",
    "location" => "casa",
    "nationalite" => "marocaine",
    "city" => "casa",
    "tel" => "0787979778",
    "username" => "@aymen"
];


$item = $marshaler->marshalJson(json_encode($data));

$params = [
    'TableName' => 'Forms2', 
    'Item' => $item
];

try {
    $client->putItem($params);
    echo "✅ Donnée insérée avec succès.\n";
} catch (DynamoDbException $e) {
    echo "❌ Erreur : " . $e->getMessage() . "\n";
}
