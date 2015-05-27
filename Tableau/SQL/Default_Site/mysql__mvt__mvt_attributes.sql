/* MySQL - Index */
SELECT
	m.id AS multivariate_test_attribute_id,
	m.site_id,
	s.name AS site_name,
	m.ad_type,
	m.name AS multivariate_test_attribute_name
FROM multivariate_test_attributes m
LEFT JOIN sites s
ON m.site_id = s.id