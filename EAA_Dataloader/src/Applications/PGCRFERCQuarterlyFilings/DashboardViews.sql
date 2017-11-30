--Required for the Asset Dashboard.
CREATE VIEW pgcr_dev.vw_qf_product_eiaid 
AS
SELECT *
FROM pgcr_prod.qf_contracts a
  JOIN pgcr_dev.ihsmarkitdata_product_map b
    ON lower (a.product_name) = lower (b.productnameraw)
  JOIN pgcr_dev.ihsmarkitdata_seller_eia_id c
    ON lower (a.seller_company_name) = lower (c.seller);
    
    