SELECT c."name",
       MAX(t."name") AS highest_batch_name
FROM tags t
JOIN model_tags mt ON t.id = mt.tag_id
    AND mt.deleted_at IS NULL
    AND mt.model_type = 'Entity'
JOIN entities e ON mt.model_id = e.id
    AND e.deleted_at IS NULL
JOIN campaign_targets ct ON e.id = ct.entity_id
    AND ct.deleted_at IS NULL
JOIN campaigns c ON ct.campaign_id = c.id
    AND c.deleted_at IS NULL
WHERE t.deleted_at IS NULL
  AND t."name" LIKE 'Batch%'
GROUP BY c."name";
