WITH case_tags AS (
SELECT
	mt.model_id AS pleteo_case_id,
	max(t.name) FILTER (WHERE t.tag_category_id = 1) AS country, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 3) AS region, --Category
	max(t.name) FILTER (WHERE t.tag_category_id = 2) AS batch, --Category
	max(t.name) FILTER (WHERE t.name IN ('EDU','GOV')) AS unq_reason,
	max(t.name) FILTER (WHERE t.name = 'Investigation On-Hold') AS investigation_on_hold,
	string_agg(t.name, ', ' ORDER BY t.id ASC) FILTER (WHERE t.tag_category_id = 4) AS chaos_decision --Category
FROM model_tags mt
JOIN tags t ON mt.tag_id = t.id AND t.deleted_at IS NULL
WHERE mt.deleted_at IS NULL
AND mt.model_type = 'Case'
GROUP BY 1
)
SELECT
ctags.country,
c.entity_id AS "entity.id",
c.case_id AS "case.id",
'Ready for Engagement' AS "case.investigation_status",
--Account Manager Fields
CASE
	WHEN ctags.country = 'Peru' THEN 'Marisol Olvera'
	WHEN ctags.country = 'Philippines' THEN 'Villy Hinayas'
	WHEN ctags.country = 'UAE' THEN 'Chowdhury Mehboob'
	WHEN ctags.country = 'Japan' THEN 'Sawa Suzuki'
END AS "entity.account_manager", -- Custom Field
CASE
	WHEN ctags.country = 'Peru' THEN 'marisol@connor-consulting.com'
	WHEN ctags.country = 'Philippines' THEN 'villy@connor-consulting.com'
	WHEN ctags.country = 'UAE' THEN 'mehboob@connor-consulting.com'
	WHEN ctags.country = 'Japan' THEN 'sawa@connor-consulting.com'
END AS "entity.account_managers", -- Proper account manager table
CASE
	WHEN ctags.country = 'Peru' THEN '52 55-3878-8378'
	WHEN ctags.country = 'Philippines' THEN '63 99-4264-3470'
	WHEN ctags.country = 'UAE' THEN '91 80-1071-0148'
	WHEN ctags.country = 'Japan' THEN '81 90-7699-2893'
END AS "entity.account_manager_phone", -- Custom Field
CASE
	WHEN ctags.country = 'Peru' THEN 'marisol@connor-consulting.com'
	WHEN ctags.country = 'Philippines' THEN 'villy@connor-consulting.com'
	WHEN ctags.country = 'UAE' THEN 'mehboob@connor-consulting.com'
	WHEN ctags.country = 'Japan' THEN 'sawa@connor-consulting.com'
END AS "entity.account_manager_email", -- Custom Field
--Entity Tags
-- Mejorable: batch tiene que ser buscado de un input al principio/lista
CASE
	WHEN ctags.region IS NOT NULL THEN ctags.country||','||ctags.region||','||'Batch {}'||','||'Send Scheduled Emails'
	ELSE ctags.country||','||'Batch {}'||','||'Send Scheduled Emails'
END AS "entity.tags",
-- Other Entity Custom Fields
NULL AS "entity.years_machines_approved_for_engagement", --we GET this FROM a google sheet we have WITH the client , vlookup IN the LAST file
NULL AS "case.years_machines_approved_for_engagement", -- same than FOR entities
'Client Approved'||','||'Batch {}' AS "case.tags", -- we have TO improve it AS we mentiones FOR entity batch
--Revenue Opportunity
'Client' AS "revenue_opportunity.recipient_type",
'01. Notification (System)' AS "revenue_opportunity.status",
'V-Ray' AS "revenue_opportunity.invoice_item.0.product",
3 AS "revenue_opportunity.invoice_item.0.term",
NULL AS "revenue_opportunity.invoice_item.0.quantity",
CASE
	WHEN ctags.country = 'Peru' THEN 'Marisol Olvera' 
	WHEN ctags.country = 'Philippines' THEN 'Villy Hinayas'
	WHEN ctags.country = 'UAE' THEN 'Chowdhury Mehboob'
	WHEN ctags.country = 'Japan' THEN 'Sawa Suzuki'
END AS
"revenue_opportunity.ro_account_manager",
'2024-11-19' AS "revenue_opportunity.effective_date" --Date IN which we launch
FROM cases c
JOIN case_tags ctags ON c.id = ctags.pleteo_case_id
WHERE c.deleted_at IS NULL
AND ctags.country = '{}' -- pais, una busqueda por cada uno
AND c.case_id in ({}) -- cases_id input
ORDER BY 1,2;
