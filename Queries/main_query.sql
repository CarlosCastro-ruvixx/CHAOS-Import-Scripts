WITH entity_tags AS (
SELECT
	mt.model_id AS entity_id,
	max(t.name) FILTER (WHERE t.tag_category_id = 1) AS country, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 3) AS region, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 2) AS batch, --Category
	max(t.name) FILTER (WHERE t.name IN ('EDU','GOV')) AS unq_reason,
	max(t.name) FILTER (WHERE t.name = 'Investigation On-Hold') AS investigation_on_hold,
	string_agg(t.name, ', ' ORDER BY t.id ASC) FILTER (WHERE t.tag_category_id = 4) AS chaos_decision --Category
FROM model_tags mt
JOIN tags t ON mt.tag_id = t.id AND t.deleted_at IS NULL
WHERE mt.deleted_at IS NULL
AND mt.model_type = 'Entity'
GROUP BY 1
)
, contact_tags AS (
SELECT
	mt.model_id AS contact_id,
	max(t.name) FILTER (WHERE t.tag_category_id = 1) AS country, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 3) AS region, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 2) AS batch, --Category
	max(t.name) FILTER (WHERE t.name IN ('EDU','GOV')) AS unq_reason,
	max(t.name) FILTER (WHERE t.name = 'Investigation On-Hold') AS investigation_on_hold,
	string_agg(t.name, ', ' ORDER BY t.id ASC) FILTER (WHERE t.tag_category_id = 4) AS chaos_decision --Category
FROM model_tags mt
JOIN tags t ON mt.tag_id = t.id AND t.deleted_at IS NULL
WHERE mt.deleted_at IS NULL
AND mt.model_type = 'Contact'
GROUP BY 1
)
SELECT
etags.entity_id,
etags.country,
c.id AS "contact.id",
CASE
	WHEN etags.region IS NOT NULL THEN etags.country||','||etags.region||','||'Batch {}'||','||'Send Scheduled Emails' ---- se debe cambiar al batch correspondiente
	ELSE etags.country||','||'Batch {}'||','||'Send Scheduled Emails' -----!!!!----  se debe cambiar al batch correspondiente
END AS "contact.tags"
FROM contacts c
JOIN entity_tags etags ON etags.entity_id = c.parent_entity_id
WHERE NOT EXISTS (SELECT 1 FROM contact_tags ctags WHERE ctags.contact_id = c.id AND ctags.batch IS NOT NULL) --esta se comenta y se incluyen los contactos ya taggeados y la siguiente linea se modifica con un where (de and)
and c.deleted_at IS NULL
AND EXISTS (SELECT 1 FROM cases  WHERE c.parent_entity_id = cases.entity_id AND cases.case_id IN (
'4114679#1','4104751#4','4710567#1','4944919#1','5040528#1','3252211#2','4393871#2','1796306#3','1950813#2','3991009#2','4073878#1','522658#3','4359915#1','1827679#2','4048883#1','4710590#1','4767960#1','931242#2','4924225#1','4837114#1'))
AND etags.country ='{}' -----!!!!----- hay que cambiar a la region que corresponda
ORDER BY 2,1,3;
