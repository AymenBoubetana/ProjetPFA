#!/usr/bin/env node

const { DynamoDBClient, ScanCommand } = require('@aws-sdk/client-dynamodb');
const { unmarshall } = require('@aws-sdk/util-dynamodb');
const { performance } = require('perf_hooks');
const fs = require('fs');

const AWS_CONFIG = {
  region: 'eu-north-1',
  credentials: {
    accessKeyId: 'Access_key',
    secretAccessKey: 'Access_secret'
  },
  maxRetries: 3
};

const TABLE_NAME = 'testRempliHuge';
const SEARCH_TERM = "F_10016.pdf";
const BATCH_SIZE = 1000;
const MAX_DOCUMENTS = 20000;
const PARALLEL_WORKERS = 8; // Number of parallel scan segments

async function scanSegment(segment, totalSegments) {
  const client = new DynamoDBClient(AWS_CONFIG);
  const foundDocs = [];
  let scannedCount = 0;
  let matchCount = 0;
  let lastEvaluatedKey = null;

  do {
    const result = await client.send(
      new ScanCommand({
        TableName: TABLE_NAME,
        Limit: BATCH_SIZE,
        Segment: segment,
        TotalSegments: totalSegments,
        ExclusiveStartKey: lastEvaluatedKey
      })
    );

    for (const item of result.Items) {
      const doc = unmarshall(item);
      scannedCount++;

      if (Array.isArray(doc.decode_template)) {
        const hasPartialMatch = doc.decode_template.some(
          field =>
            typeof field?.field_value === 'string' &&
            field.field_value.includes(SEARCH_TERM)
        );

        if (hasPartialMatch) {
          matchCount++;
          foundDocs.push(doc);
          if (foundDocs.length >= MAX_DOCUMENTS) break;
        }
      }
    }

    lastEvaluatedKey = result.LastEvaluatedKey;
  } while (lastEvaluatedKey && foundDocs.length < MAX_DOCUMENTS);

  client.destroy();
  return { foundDocs, scannedCount, matchCount };
}

(async () => {
  const startTime = performance.now();

  // Run all scan segments in parallel
  const segmentResults = await Promise.all(
    Array.from({ length: PARALLEL_WORKERS }, (_, i) =>
      scanSegment(i, PARALLEL_WORKERS)
    )
  );

  // Combine results
  const allDocs = segmentResults.flatMap(r => r.foundDocs);
  const totalScanned = segmentResults.reduce((sum, r) => sum + r.scannedCount, 0);
  const totalMatches = segmentResults.reduce((sum, r) => sum + r.matchCount, 0);

  const duration = (performance.now() - startTime) / 1000;
  console.log(`\n✅ Scan completed in ${duration.toFixed(2)}s`);
  console.log(`📊 Total scanned: ${totalScanned}`);
  console.log(`🎯 Matches found: ${totalMatches}`);

  if (totalMatches > 0) {
    const filename = `dynamodb_parallel_matches_${Date.now()}.json`;
    fs.writeFileSync(filename, JSON.stringify(allDocs, null, 2));
    console.log(`💾 Saved results to: ${filename}`);
  }
})();








