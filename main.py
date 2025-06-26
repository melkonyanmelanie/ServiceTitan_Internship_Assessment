import pickle
import pandas as pd
import os

class CustomerDataExtractor:
    def __init__(self, customer_orders_path, vip_customers_path):
        self.customer_orders_path = customer_orders_path
        self.vip_customers_path = vip_customers_path
        self.customer_orders = None
        self.vip_ids = None
        self.load_data()

    def load_data(self):
        with open(self.customer_orders_path, 'rb') as f:
            self.customer_orders = pickle.load(f)
        with open(self.vip_customers_path, 'r') as f:
            self.vip_ids = {int(line.strip()) for line in f if line.strip().isdigit()}

    def clean_order_id(self, order_id):
        if order_id is None:
            print("Skipping None order_id")
            return None
        if isinstance(order_id, int):
            return order_id
        if isinstance(order_id, str) and order_id.startswith('ORD'):
            try:
                return int(order_id[3:])
            except ValueError:
                print(f"Skipping invalid order_id: {order_id}")
                return None
        try:
            return int(order_id)
        except Exception:
            print(f"Skipping unrecognized order_id: {order_id}")
            return None

    def normalize_category(self, category):
        mapping = {
            1: 'Electronics',
            2: 'Apparel',
            3: 'Books',
            4: 'Home Goods'
        }
        if isinstance(category, int):
            return mapping.get(category, 'Misc')
        return 'Misc'

    def safe_to_float(self, value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def quantity_to_int(self, quantity):
        if isinstance(quantity, int):
            return quantity
        if isinstance(quantity, str):
            quantity = quantity.strip()
            if quantity.isdigit():
                return int(quantity)
        return None

    def transform_data(self):
        records = []

        for customer in self.customer_orders:
            try:
                customer_id = customer.get('id') or customer.get('customer_id')
                if customer_id is None:
                    print("Skipping customer with None customer_id")
                    continue
                customer_id = int(customer_id)
                customer_name = customer.get('name') or customer.get('customer_name', '')
                registration_date_raw = customer.get('registration_date')
                try:
                    registration_date = pd.to_datetime(registration_date_raw)
                except Exception:
                    print(f"Skipping customer with invalid registration_date: {registration_date_raw}")
                    continue

                is_vip = customer_id in self.vip_ids

                orders = customer.get('orders', [])
                for order in orders:
                    raw_order_id = order.get('order_id')
                    order_id = self.clean_order_id(raw_order_id)
                    if order_id is None:
                        continue
                    raw_order_date = order.get('order_date')
                    try:
                        order_date = pd.to_datetime(raw_order_date)
                    except Exception:
                        print(f"Skipping order with invalid order_date: {raw_order_date}")
                        continue

                    items = order.get('items', [])
                    valid_items = []
                    total_order_value = 0

                    # Sum total order value including negatives
                    for item in items:
                        unit_price = self.safe_to_float(item.get('price') or item.get('unit_price'))
                        quantity = self.quantity_to_int(item.get('quantity') or item.get('item_quantity'))

                        if unit_price is None or quantity is None:
                            continue

                        if quantity == 0 or unit_price == 0:
                            continue

                        total_item_price = unit_price * quantity
                        valid_items.append((item, unit_price, quantity, total_item_price))
                        total_order_value += total_item_price  # include negatives as is

                    if abs(total_order_value) < 1e-6:
                        # Skip orders with zero or near-zero total value to avoid division issues
                        continue

                    for item, unit_price, quantity, total_item_price in valid_items:
                        total_order_value_percentage = total_item_price / total_order_value

                        product_id = item.get('item_id') or item.get('product_id')
                        if product_id is None:
                            print("Skipping item with None product_id")
                            continue
                        try:
                            product_id = int(product_id)
                        except Exception:
                            print(f"Skipping item with invalid product_id: {product_id}")
                            continue

                        product_name = item.get('product_name', '')
                        raw_category = item.get('category')
                        category = self.normalize_category(raw_category)

                        record = {
                            'customer_id': customer_id,
                            'customer_name': str(customer_name),
                            'registration_date': registration_date,
                            'is_vip': is_vip,
                            'order_id': order_id,
                            'order_date': order_date,
                            'product_id': product_id,
                            'product_name': str(product_name),
                            'category': category,
                            'unit_price': unit_price,
                            'item_quantity': quantity,
                            'total_item_price': total_item_price,
                            'total_order_value_percentage': total_order_value_percentage
                        }
                        records.append(record)

            except Exception as e:
                print(f"Skipping customer due to error: {e}")
                continue

        df = pd.DataFrame(records)
        if df.empty:
            print("No valid data to transform.")
            return df

        # enforce types
        df = df.astype({
            'customer_id': 'int',
            'customer_name': 'string',
            'registration_date': 'datetime64[ns]',
            'is_vip': 'bool',
            'order_id': 'int',
            'order_date': 'datetime64[ns]',
            'product_id': 'int',
            'product_name': 'string',
            'category': 'string',
            'unit_price': 'float',
            'item_quantity': 'int',
            'total_item_price': 'float',
            'total_order_value_percentage': 'float'
        })

        df = df.sort_values(by=['customer_id', 'order_id', 'product_id']).reset_index(drop=True)
        return df

    def save_to_csv(self, df, filename):
        df.to_csv(filename, index=False)
        print(f"DataFrame saved to {filename}")


def main():
    base_path = os.path.dirname(__file__)  

    customer_orders_file = os.path.join(base_path, 'customer_orders.pkl')
    vip_customers_file = os.path.join(base_path, 'vip_customers.txt')

    extractor = CustomerDataExtractor(customer_orders_file, vip_customers_file)
    df = extractor.transform_data()
    output_path = os.path.join(base_path, 'transformed_customer_orders.csv')
    extractor.save_to_csv(df, output_path)

if __name__ == "__main__":
    main()