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


$primaryKey = 'code_synchronisations';
$primaryKeyValue = 'aymen-001'; 
// 2. LECTURE (GetItem)
// -----------------------------
$key = $marshaler->marshalJson(json_encode([
    $primaryKey => $primaryKeyValue
]));

try {
    $result = $client->getItem([
        'TableName' => 'Forms2',
        'Key' => $key
    ]);

    if (isset($result['Item'])) {
        $item = $marshaler->unmarshalItem($result['Item']);
        echo "📦 Donnée lue :\n";
        print_r($item);
    } else {
        echo "ℹ️ Aucune donnée trouvée avec la clé $primaryKeyValue.\n";
    }
} catch (DynamoDbException $e) {
    echo "❌ Erreur lecture : " . $e->getMessage() . "\n";
}