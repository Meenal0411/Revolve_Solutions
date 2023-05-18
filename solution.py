#Import all packages here
import argparse
import csv
import json
import os


def get_params() -> dict:
    """
    Retrieves command-line parameters using argparse.
    Returns a dictionary of the parameters.
    """
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def process_data(customers_location, products_location, transactions_location, output_location):
    """
    Processes the input data to generate output JSON files for each customer.
    """

    # Step 1: Load customers' data from CSV
    customers = load_customers(customers_location)

    # Step 2: Load products' data from CSV
    products = load_products(products_location)

    # Step 3: Process transactions data
    for file_name in os.listdir(transactions_location):
        if file_name.endswith('.jsonl'):
            file_path = os.path.join(transactions_location, file_name)
            process_transaction_file(file_path, customers, products)

    # Step 4: Generate output JSON files for each customer
    generate_output_files(customers, output_location)


def load_customers(customers_location):
    """
    Loads customers' data from a CSV file and returns a dictionary of customers with their loyalty scores.
    """
    customers = {}
    with open(customers_location, 'r') as customers_file:
        reader = csv.DictReader(customers_file)
        for row in reader:
            customer_id = row['customer_id']
            loyalty_score = row['loyalty_score']
            customers[customer_id] = {'loyalty_score': loyalty_score, 'purchases': []}
    return customers


def load_products(products_location):
    """
    Loads products' data from a CSV file and returns a dictionary of products with their categories.
    """
    products = {}
    with open(products_location, 'r') as products_file:
        reader = csv.DictReader(products_file)
        for row in reader:
            product_id = row['product_id']
            product_category = row['product_category']
            products[product_id] = {'product_category': product_category}
    return products


def process_transaction_file(file_path, customers, products):
    """
    Processes a transaction file in JSON Lines format and updates the customers' purchase records.
    """
    with open(file_path, 'r') as transactions_file:
        for line in transactions_file:
            transaction = json.loads(line)
            customer_id = transaction['customer_id']
            product_id = transaction['product_id']
            purchase_count = transaction['purchase_count']

            if customer_id in customers:
                customers[customer_id]['purchases'].append({
                    'product_id': product_id,
                    'purchase_count': purchase_count
                })


def generate_output_files(customers, output_location):
    """
    Generates output JSON files for each customer containing the required information.
    """
    for customer_id, customer_data in customers.items():
        output_data = {
            'customer_id': customer_id,
            'loyalty_score': customer_data['loyalty_score'],
            'purchases': []
        }
        for purchase in customer_data['purchases']:
            product_id = purchase['product_id']
            purchase_count = purchase['purchase_count']
            product_category = products.get(product_id, {}).get('product_category', '')
            output_data['purchases'].append({
                'product_id': product_id,
                'product_category': product_category,
                'purchase_count': purchase_count
            })

        output_file_path = os.path.join(output_location, f"customer_{customer_id}.json")
        with open(output_file_path, 'w') as output_file:
            json.dump(output_data, output_file, indent=2)


def main():
    try:
        # Step 0: Retrieve command-line parameters
        params = get_params()
        customers_location = params['customers_location']
        products_location = params['products_location']
        transactions_location = params['transactions_location']
        output_location = params['output_location']

        # Step 5: Process the data
        process_data(customers_location, products_location, transactions_location, output_location)

        print("Data processing completed successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()

