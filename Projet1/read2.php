<?php

require 'vendor/autoload.php';

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;

function get_form_by_id($id)
{
    $client = new DynamoDbClient([
        'region' => 'eu-north-1',
        'version' => 'latest',
        'credentials' => [
            'key' => 'Access_key',
            'secret' => 'Access_secret',
        ]
    ]);

    $marshaler = new Marshaler();

    try {
        $key = $marshaler->marshalJson(json_encode(['id' => (int)$id]));

        $result = $client->getItem([
            'TableName' => 'forms_bis',
            'Key' => $key,
        ]);

        if (isset($result['Item'])) {
            return $marshaler->unmarshalItem($result['Item']);
        }

        return null;
    } catch (\Exception $e) {
        return null;
    }
}


$form = get_form_by_id(17872); // remplace avec un ID existant dans ta table

if ($form) {
    echo "<pre>";
    print_r($form);
    echo "</pre>";
} else {
    echo "Aucun formulaire trouvé pour cet ID.";
}
