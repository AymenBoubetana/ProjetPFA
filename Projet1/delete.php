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

// 4. SUPPRESSION (DeleteItem)
// -----------------------------
try {
    $client->deleteItem([
        'TableName' => 'Forms2',
        'Key' => [
            $primaryKey => ['S' => $primaryKeyValue]
        ]
    ]);

    echo "🗑️ Élément supprimé avec succès (code_synchronisations = $primaryKeyValue).\n";
} catch (DynamoDbException $e) {
    echo "❌ Erreur suppression : " . $e->getMessage() . "\n";
}