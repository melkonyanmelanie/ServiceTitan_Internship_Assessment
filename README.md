Conversation: https://chatgpt.com/share/685d512f-8438-8013-aba3-e31b5d223a37 + Google search and cases


# This code helped me understand whether I have issues in the given entities or not (and then proceed with writing some logic to prevent errors)
from pprint import pprint

# Initialize sets for all target columns
customer_ids = set()
customer_names = set()
registration_dates = set()
is_vip_flags = set()  # Only present if you include VIP info

order_ids = set()
order_dates = set()

product_ids = set()
product_names = set()
categories = set()
unit_prices = set()
item_quantities = set()
total_item_prices = set()
total_order_value_percentages = set()

# Iterate through all customers and extract data
for customer in data:
    customer_ids.add(customer.get("id"))
    customer_names.add(customer.get("name"))
    registration_dates.add(customer.get("registration_date"))
    is_vip_flags.add(customer.get("is_vip"))  # Optional: if not available, ignore

    for order in customer.get("orders", []):
        order_ids.add(order.get("order_id"))
        order_dates.add(order.get("order_date"))
        
        total_order_value = order.get("order_total_value") or 1  # avoid div by zero
        for item in order.get("items", []):
            product_ids.add(item.get("item_id"))
            product_names.add(item.get("product_name"))
            categories.add(item.get("category"))

            price = item.get("price")
            quantity = item.get("quantity")

            unit_prices.add(price)
            item_quantities.add(quantity)

            # Compute total item price if values are valid
            try:
                clean_price = float(str(price).replace("$", "").strip())
                clean_quantity = int(str(quantity).strip()) if 'free' not in str(quantity).lower() else 0
                total_price = clean_price * clean_quantity
                percent = total_price / total_order_value
            except:
                total_price = None
                percent = None

            total_item_prices.add(total_price)
            total_order_value_percentages.add(percent)

# Print sample results (you can remove `[:20]` to see everything)
print("\nCustomer IDs:")
pprint(customer_ids)

print("\nCustomer Names:")
pprint(customer_names)

print("\nRegistration Dates:")
pprint(registration_dates)

print("\nIs VIP Flags:")
pprint(is_vip_flags)

print("\nOrder IDs:")
pprint(order_ids)

print("\nOrder Dates:")
pprint(order_dates)

print("\nProduct IDs:")
pprint(product_ids)

print("\nProduct Names:")
pprint(product_names)

print("\nCategories:")
pprint(categories)

print("\nUnit Prices:")
pprint(unit_prices)

print("\nItem Quantities:")
pprint(item_quantities)

print("\nTotal Item Prices:")
pprint(total_item_prices)

print("\nTotal Order Value Percentages:")
pprint(total_order_value_percentages)
