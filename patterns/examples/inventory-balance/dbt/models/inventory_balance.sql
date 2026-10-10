select sku, warehouse_code, cast(available_quantity as integer) as available_quantity
from (
    values
        ('synthetic-sku-001', 'DEMO-NORTH', 12),
        ('synthetic-sku-002', 'DEMO-NORTH', 7),
        ('synthetic-sku-003', 'DEMO-SOUTH', 0)
) as inventory(sku, warehouse_code, available_quantity)