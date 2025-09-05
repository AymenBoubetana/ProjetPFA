<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Aws\DynamoDb\DynamoDbClient;
use GuzzleHttp\Client;

class SyncFormsToDynamo extends Command
{
    protected $signature = 'sync:forms 
        {--create : Créer des formulaires depuis l\'API}
        {--read= : Lire un formulaire par ID}
        {--update= : Mettre à jour un formulaire par ID}
        {--delete= : Supprimer un formulaire par ID}';

    protected $description = 'CRUD pour les formulaires stockés dans DynamoDB';
    protected $httpClient;
    protected $dynamoClient;
    protected $baseUrl;
    protected $tableName;

    public function __construct()
    {
        parent::__construct();

        $this->httpClient = new Client([
            'verify' => false,
            'headers' => [
                'Authorization' => 'Bearer Access_token,
                'Accept' => 'application/json'
            ]
        ]);

        $this->dynamoClient = new DynamoDbClient([
            'region' => env('AWS_DEFAULT_REGION'),
            'version' => 'latest',
            'credentials' => [
                'key' => env('AWS_ACCESS_KEY_ID'),
                'secret' => env('AWS_SECRET_ACCESS_KEY'),
            ],
        ]);

        $this->baseUrl = 'https://api.vengoreserve.com/api';
        $this->tableName = env('AWS_DYNAMODB_TABLE', 'Forms2');
    }

    public function handle()
    {
        if ($this->option('create')) {
            $this->createForms();
        } elseif ($id = $this->option('read')) {
            $this->readForm($id);
        } elseif ($id = $this->option('update')) {
            $this->updateForm($id);
        } elseif ($id = $this->option('delete')) {
            $this->deleteForm($id);
        } else {
            $this->error("Aucune option spécifiée. Utilise --create, --read=ID, --update=ID ou --delete=ID");
        }
    }

    // CRUD: CREATE
    private function createForms()
    {
        $this->info("📥 Fetching forms from API...");
        $formsData = $this->fetchAllForms();

        if (!$formsData || !isset($formsData['myforms'])) {
            $this->error("Aucun formulaire trouvé.");
            return;
        }

        foreach ($formsData['myforms'] as $form) {
            $this->storeForm($form);
            usleep(200000);
        }

        $this->info("✅ Tous les formulaires ont été créés.");
    }

    // CRUD: READ
    private function readForm($id)
    {
        try {
            $result = $this->dynamoClient->getItem([
                'TableName' => $this->tableName,
                'Key' => [
    'code_synchronisations' => ['S' => $id]
]
            ]);

            if (!isset($result['Item'])) {
                $this->warn("Aucun formulaire trouvé avec l'ID $id.");
                return;
            }

            $this->info("📄 Formulaire $id trouvé :");
            print_r($result['Item']);
        } catch (\Exception $e) {
            $this->error("Erreur lecture formulaire : " . $e->getMessage());
        }
    }

    // CRUD: UPDATE
    private function updateForm($id)
    {
        $field = $this->ask("Quel champ veux-tu mettre à jour ?");
        $value = $this->ask("Nouvelle valeur pour $field ?");

        try {
            $this->dynamoClient->updateItem([
                'TableName' => $this->tableName,
                'Key' => [
    'code_synchronisations' => ['S' => $id]
],
                'UpdateExpression' => "SET #f = :v",
                'ExpressionAttributeNames' => ['#f' => $field],
                'ExpressionAttributeValues' => [':v' => ['S' => $value]],
            ]);

            $this->info("✅ Formulaire $id mis à jour avec $field = $value");
        } catch (\Exception $e) {
            $this->error("Erreur mise à jour : " . $e->getMessage());
        }
    }

    // CRUD: DELETE
    private function deleteForm($id)
    {
        try {
            $this->dynamoClient->deleteItem([
                'TableName' => $this->tableName,
                'Key' => [
    'code_synchronisations' => ['S' => $id]
]
            ]);

            $this->info("🗑️ Formulaire $id supprimé de DynamoDB.");
        } catch (\Exception $e) {
            $this->error("Erreur suppression : " . $e->getMessage());
        }
    }

    // ============ API FUNCTIONS ============

    private function fetchAllForms()
    {
        try {
            $response = $this->httpClient->get($this->baseUrl . '/view/allforms?total=6500');
            return json_decode($response->getBody(), true);
        } catch (\Exception $e) {
            $this->error("Erreur appel API : " . $e->getMessage());
            return null;
        }
    }

    private function fetchFormDetails($id)
    {
        try {
            $response = $this->httpClient->get("{$this->baseUrl}/view/form/{$id}");
            $data = json_decode($response->getBody(), true);
            return $data['myform']['decode_template'] ?? [];
        } catch (\Exception $e) {
            return [];
        }
    }

    private function storeForm($form)
    {
        $formId = $form['id'] ?? null;
        $code = $form['code_synchronisations'] ?? null;

        if (!$formId || !$code) {
            $this->warn("Formulaire invalide.");
            return;
        }

        $form['template'] = $this->fetchFormDetails($formId);

        $item = [];
        foreach ($form as $key => $value) {
            if (is_null($value)) continue;

            if (is_scalar($value)) {
                $item[$key] = ['S' => strval($value)];
            } elseif (is_array($value)) {
                if ($this->isAssociative($value)) {
                    $item[$key] = ['M' => $this->convertToDynamoMap($value)];
                } else {
                    $item[$key] = ['L' => $this->convertToDynamoList($value)];
                }
            } else {
                $item[$key] = ['S' => json_encode($value)];
            }
        }

        $this->dynamoClient->putItem([
            'TableName' => $this->tableName,
            'Item' => $item
        ]);

        $this->info("✅ Formulaire $formId stocké.");
    }

    private function isAssociative(array $arr)
    {
        return array_keys($arr) !== range(0, count($arr) - 1);
    }

    private function convertToDynamoList(array $arr)
    {
        $list = [];
        foreach ($arr as $value) {
            if (is_scalar($value)) {
                $list[] = ['S' => strval($value)];
            } elseif (is_array($value)) {
                $list[] = $this->isAssociative($value)
                    ? ['M' => $this->convertToDynamoMap($value)]
                    : ['L' => $this->convertToDynamoList($value)];
            } else {
                $list[] = ['S' => json_encode($value)];
            }
        }
        return $list;
    }

    private function convertToDynamoMap(array $arr)
    {
        $map = [];
        foreach ($arr as $key => $value) {
            if (is_scalar($value)) {
                $map[$key] = ['S' => strval($value)];
            } elseif (is_array($value)) {
                $map[$key] = $this->isAssociative($value)
                    ? ['M' => $this->convertToDynamoMap($value)]
                    : ['L' => $this->convertToDynamoList($value)];
            } else {
                $map[$key] = ['S' => json_encode($value)];
            }
        }
        return $map;
    }
}
