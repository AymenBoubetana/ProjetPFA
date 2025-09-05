<?php
require 'vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Exception\DynamoDbException;

// Fonction de mise à jour
function update_form_by_id($id, $newData) {
    $client = new DynamoDbClient([
        'region' => 'eu-north-1',
        'version' => 'latest',
        'credentials' => [
            'key' => 'Access_key',
            'secret' => 'Access_secret',
        ]
    ]);

    try {
        $updateExpression = 'SET ';
        $expressionAttributeValues = [];
        $expressionAttributeNames = [];

        foreach ($newData as $key => $value) {
            $updateExpression .= "#$key = :$key, ";
            $expressionAttributeNames["#$key"] = $key;
            $expressionAttributeValues[":$key"] = ['S' => $value];
        }

        $updateExpression = rtrim($updateExpression, ', ');

        $client->updateItem([
            'TableName' => 'forms_bis',
            'Key' => [
                'id' => ['N' => (string)$id],
            ],
            'UpdateExpression' => $updateExpression,
            'ExpressionAttributeNames' => $expressionAttributeNames,
            'ExpressionAttributeValues' => $expressionAttributeValues,
        ]);

        echo "✅ Formulaire avec ID $id mis à jour avec succès.\n";
    } catch (DynamoDbException $e) {
        echo "❌ Erreur lors de la mise à jour : " . $e->getMessage() . "\n";
    }
}

// Fonction de suppression
function delete_form_by_id($id) {
    $client = new DynamoDbClient([
        'region' => 'eu-north-1',
        'version' => 'latest',
        'credentials' => [
            'key' => 'Access_key',
            'secret' => 'Access_secret',
        ]
    ]);

    try {
        $client->deleteItem([
            'TableName' => 'forms_bis',
            'Key' => [
                'id' => ['N' => (string)$id],
            ],
        ]);

        echo "✅ Formulaire avec ID $id supprimé avec succès.\n";
    } catch (DynamoDbException $e) {
        echo "❌ Erreur lors de la suppression : " . $e->getMessage() . "\n";
    }
}

// ----------------------------------------------
// 🎯 Appels de test
// ----------------------------------------------

// Exemple de mise à jour
update_form_by_id(17868, [
    'account_name' => 'Compte Ayman Modifié',
    'code_synchronisations' => 'SYNC_TEST_999'
]);

// Exemple de suppression
// delete_form_by_id(17866);
