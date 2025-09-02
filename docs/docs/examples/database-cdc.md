# Database CDC Worker Example

This example demonstrates how to create a Change Data Capture (CDC) worker that monitors PostgreSQL database changes in real-time.

## Scenario

We'll build an e-commerce system that:
- Monitors order changes in PostgreSQL
- Sends email confirmations for new orders
- Updates inventory when orders are placed
- Tracks order status changes

## Code

```python
import asyncio
from typing import Any
from pythia.brokers.database import PostgreSQLCDCWorker, DatabaseChange, ChangeType

class ECommerceCDCWorker(PostgreSQLCDCWorker):
    """
    E-commerce CDC worker that processes order changes
    """

    def __init__(self):
        super().__init__(
            connection_string="postgresql://user:password@localhost:5432/ecommerce",
            tables=["orders", "order_items", "inventory"],
            slot_name="ecommerce_cdc_slot",
            publication_name="ecommerce_publication"
        )

    async def process_change(self, change: DatabaseChange) -> Any:
        """Process database changes based on table and operation"""

        if change.table == "orders":
            return await self._process_order_change(change)
        elif change.table == "order_items":
            return await self._process_order_item_change(change)
        elif change.table == "inventory":
            return await self._process_inventory_change(change)

        return {"processed": False, "reason": "Unknown table"}

    async def _process_order_change(self, change: DatabaseChange) -> dict:
        """Process order table changes"""

        if change.change_type == ChangeType.INSERT:
            # New order created
            order_data = change.new_data
            self.logger.info(f"New order created: {order_data.get('id')}")

            # Send order confirmation email
            await self._send_order_confirmation(order_data)

            # Update inventory for ordered items
            await self._reserve_inventory(order_data['id'])

            return {
                "action": "new_order_processed",
                "order_id": order_data.get('id'),
                "customer_email": order_data.get('customer_email'),
                "total_amount": order_data.get('total_amount')
            }

        elif change.change_type == ChangeType.UPDATE:
            # Order status changed
            old_status = change.old_data.get('status')
            new_status = change.new_data.get('status')

            if old_status != new_status:
                self.logger.info(f"Order {change.new_data.get('id')} status: {old_status} → {new_status}")
                await self._handle_status_change(change.new_data, old_status, new_status)

            return {
                "action": "status_change",
                "order_id": change.new_data.get('id'),
                "old_status": old_status,
                "new_status": new_status
            }

        elif change.change_type == ChangeType.DELETE:
            # Order cancelled/deleted
            order_data = change.old_data
            self.logger.info(f"Order deleted: {order_data.get('id')}")

            # Release reserved inventory
            await self._release_inventory(order_data['id'])

            return {
                "action": "order_deleted",
                "order_id": order_data.get('id')
            }

    async def _process_order_item_change(self, change: DatabaseChange) -> dict:
        """Process order items changes"""

        if change.change_type == ChangeType.INSERT:
            # New item added to order
            item_data = change.new_data

            # Update product analytics
            await self._update_product_analytics(
                product_id=item_data.get('product_id'),
                quantity=item_data.get('quantity'),
                action='ordered'
            )

            return {
                "action": "item_added",
                "order_id": item_data.get('order_id'),
                "product_id": item_data.get('product_id'),
                "quantity": item_data.get('quantity')
            }

    async def _process_inventory_change(self, change: DatabaseChange) -> dict:
        """Process inventory changes"""

        if change.change_type == ChangeType.UPDATE:
            old_stock = change.old_data.get('stock_quantity', 0)
            new_stock = change.new_data.get('stock_quantity', 0)

            # Check for low stock alerts
            if new_stock <= change.new_data.get('low_stock_threshold', 5):
                await self._send_low_stock_alert(change.new_data)

            return {
                "action": "inventory_updated",
                "product_id": change.new_data.get('product_id'),
                "old_stock": old_stock,
                "new_stock": new_stock
            }

    # Business logic methods
    async def _send_order_confirmation(self, order_data: dict):
        """Send order confirmation email"""
        self.logger.info(f"Sending confirmation email to {order_data.get('customer_email')}")
        # Integration with email service (SendGrid, SES, etc.)
        # await email_service.send_confirmation(order_data)

    async def _reserve_inventory(self, order_id: str):
        """Reserve inventory for order items"""
        self.logger.info(f"Reserving inventory for order {order_id}")
        # Update inventory reservations
        # await inventory_service.reserve_items(order_id)

    async def _release_inventory(self, order_id: str):
        """Release reserved inventory"""
        self.logger.info(f"Releasing inventory for order {order_id}")
        # Release inventory reservations
        # await inventory_service.release_items(order_id)

    async def _handle_status_change(self, order_data: dict, old_status: str, new_status: str):
        """Handle order status changes"""
        status_handlers = {
            'paid': self._handle_payment_received,
            'shipped': self._handle_order_shipped,
            'delivered': self._handle_order_delivered,
            'cancelled': self._handle_order_cancelled
        }

        handler = status_handlers.get(new_status)
        if handler:
            await handler(order_data)

    async def _handle_payment_received(self, order_data: dict):
        """Handle payment confirmation"""
        self.logger.info(f"Payment received for order {order_data.get('id')}")
        # Trigger fulfillment process
        # await fulfillment_service.process_order(order_data['id'])

    async def _handle_order_shipped(self, order_data: dict):
        """Handle order shipment"""
        self.logger.info(f"Order {order_data.get('id')} shipped")
        # Send shipping notification
        # await notification_service.send_shipping_update(order_data)

    async def _handle_order_delivered(self, order_data: dict):
        """Handle order delivery"""
        self.logger.info(f"Order {order_data.get('id')} delivered")
        # Send delivery confirmation and request review
        # await review_service.request_review(order_data)

    async def _handle_order_cancelled(self, order_data: dict):
        """Handle order cancellation"""
        self.logger.info(f"Order {order_data.get('id')} cancelled")
        # Process refund and release inventory
        # await payment_service.process_refund(order_data['id'])

    async def _update_product_analytics(self, product_id: str, quantity: int, action: str):
        """Update product analytics"""
        # Update analytics database or send to analytics service
        self.logger.debug(f"Analytics: Product {product_id}, {action}, qty: {quantity}")

    async def _send_low_stock_alert(self, inventory_data: dict):
        """Send low stock alert"""
        product_id = inventory_data.get('product_id')
        current_stock = inventory_data.get('stock_quantity')

        self.logger.warning(f"Low stock alert: Product {product_id}, Stock: {current_stock}")
        # Send alert to inventory management team
        # await alert_service.send_low_stock_alert(inventory_data)


async def main():
    """Run the CDC worker"""
    worker = ECommerceCDCWorker()

    try:
        # Start the CDC worker
        async with worker:
            await worker.start_cdc()

            print("🚀 E-commerce CDC Worker started")
            print("📊 Monitoring: orders, order_items, inventory")
            print("📧 Features: Email confirmation, inventory tracking, analytics")
            print("Press Ctrl+C to stop...")

            # Process changes indefinitely
            async for change in worker.consume_changes():
                try:
                    result = await worker.process_change(change)
                    print(f"✅ Processed {change.change_type.value} on {change.table}: {result}")

                except Exception as e:
                    print(f"❌ Error processing change: {e}")
                    worker.logger.error(f"Processing error: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    except Exception as e:
        print(f"💥 Worker error: {e}")
    finally:
        await worker.stop_cdc()
        print("👋 CDC Worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
```

## Database Setup

### PostgreSQL Configuration

1. **Enable logical replication** in `postgresql.conf`:
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

2. **Create database and tables**:
```sql
-- Create database
CREATE DATABASE ecommerce;

-- Create tables
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id),
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2)
);

CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    low_stock_threshold INTEGER DEFAULT 5,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
ALTER USER your_user REPLICATION;
```

3. **Test data**:
```sql
-- Insert test data
INSERT INTO inventory (product_id, stock_quantity) VALUES
(1, 100), (2, 50), (3, 25);

INSERT INTO orders (customer_email, total_amount) VALUES
('customer@example.com', 99.99);

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
(1, 1, 2, 49.99);
```

## Running the Example

1. **Install dependencies**:
```bash
pip install asyncpg
```

2. **Configure connection string**:
Update the connection string in the worker to match your PostgreSQL setup.

3. **Run the worker**:
```bash
python cdc_worker.py
```

4. **Test with database changes**:
```sql
-- In another PostgreSQL session, make changes:
UPDATE orders SET status = 'paid' WHERE id = 1;
UPDATE orders SET status = 'shipped' WHERE id = 1;
UPDATE inventory SET stock_quantity = 3 WHERE product_id = 1;
```

## Expected Output

```
🚀 E-commerce CDC Worker started
📊 Monitoring: orders, order_items, inventory
📧 Features: Email confirmation, inventory tracking, analytics
Press Ctrl+C to stop...

✅ Processed UPDATE on orders: {'action': 'status_change', 'order_id': 1, 'old_status': 'pending', 'new_status': 'paid'}
✅ Processed UPDATE on orders: {'action': 'status_change', 'order_id': 1, 'old_status': 'paid', 'new_status': 'shipped'}
✅ Processed UPDATE on inventory: {'action': 'inventory_updated', 'product_id': 1, 'old_stock': 100, 'new_stock': 3}
```

## Key Features Demonstrated

1. **Real-time Processing**: Changes are processed as they happen
2. **Table-specific Logic**: Different processing for different tables
3. **Operation-specific Handling**: INSERT, UPDATE, DELETE handled differently
4. **Business Logic Integration**: Email, inventory, analytics integration points
5. **Error Handling**: Robust error handling with logging
6. **Monitoring**: Built-in logging and metrics

## Extensions

This example can be extended with:
- Integration with email services (SendGrid, AWS SES)
- Real-time analytics (ClickHouse, BigQuery)
- Message queues for async processing
- Webhook notifications
- Audit logging
- Data validation and transformation
