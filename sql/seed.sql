-- Seed Data for Reference Table: customer_types
INSERT INTO customer_types (customer_type_id, type_name, description, is_active)
VALUES
    (1, 'INDIVIDUAL', 'Retail individual consumer accounts', TRUE),
    (2, 'BUSINESS', 'Small and medium business accounts', TRUE),
    (3, 'ENTERPRISE', 'Large enterprise contractual accounts', TRUE),
    (4, 'NON_PROFIT', 'Registered charities and educational non-profit entities', TRUE),
    (5, 'INTERNAL_TEST', 'Internal staff and testing accounts', TRUE)
ON CONFLICT (customer_type_id) DO UPDATE
SET
    type_name = EXCLUDED.type_name,
    description = EXCLUDED.description,
    is_active = EXCLUDED.is_active;

