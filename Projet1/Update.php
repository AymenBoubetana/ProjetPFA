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


// 3. MISE À JOUR (UpdateItem)
// -----------------------------
try {
    $client->updateItem([
        'TableName' => 'Forms2',
        'Key' => [
            $primaryKey => ['S' => $primaryKeyValue]
        ],
        'UpdateExpression' => 'SET age = :newAge',
        'ExpressionAttributeValues' => [
            ':newAge' => ['N' => '33'] // nouvelle valeur de age
        ]
    ]);

    echo "🔄 Donnée mise à jour avec succès (age = 33).\n";
} catch (DynamoDbException $e) {
    echo "❌ Erreur mise à jour : " . $e->getMessage() . "\n";
}
