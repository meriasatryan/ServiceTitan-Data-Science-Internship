import pandas as pd
import pickle
from datetime import datetime

class InvoiceDataExtractor:
    def __init__(self, invoices_file_path, expired_invoices_file_path):
        self.invoices_file_path = invoices_file_path
        self.expired_invoices_file_path = expired_invoices_file_path

    def load_invoices(self):
        with open(self.invoices_file_path, 'rb') as invoices_file:
            self.invoices_data = pickle.load(invoices_file)

        
        with open(self.expired_invoices_file_path, 'r') as expired_invoices_file:
            expired_ids_str = expired_invoices_file.read()
            self.expired_invoice_ids = set(map(int, expired_ids_str.split(',')))

    @staticmethod
    def convert_to_int(value, default=0):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def convert_to_date(date_str, default=pd.NaT):
        try:
            return pd.to_datetime(date_str, errors='coerce')
        except (ValueError, TypeError):
            return default

    def transform_invoices(self):
        type_mapping = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        
        transformed_data = []

        for invoice in self.invoices_data:
            invoice_id = str(invoice.get('id')) 
            if not invoice_id.isdigit():
                continue  
            
            invoice_id = int(invoice_id)  
            created_date = self.convert_to_date(invoice.get('created_on'))
            items = invoice.get('items', [])

        
            total_amount = sum(
                self.convert_to_int(item['item']['unit_price']) * self.convert_to_int(item['quantity'])
                for item in items
            )

            is_expired = invoice_id in self.expired_invoice_ids

            for item_entry in items:
                item = item_entry['item']
                invoiceitem_id = item.get('id')
                invoiceitem_name = item.get('name')
                item_type = type_mapping.get(item.get('type'), 'Other')
                unit_price = self.convert_to_int(item.get('unit_price', 0))
                quantity = self.convert_to_int(item_entry.get('quantity', 1))
                total_price = unit_price * quantity
                percentage_in_invoice = total_price / total_amount if total_amount else 0

                transformed_data.append({
                    'invoice_id': invoice_id,  
                    'created_on': created_date,
                    'invoiceitem_id': invoiceitem_id,
                    'invoiceitem_name': invoiceitem_name,
                    'type': item_type,
                    'unit_price': unit_price,
                    'total_price': total_price,
                    'percentage_in_invoice': percentage_in_invoice,
                    'is_expired': is_expired
                })

        df = pd.DataFrame(transformed_data)
        
        df['invoice_id'] = df['invoice_id'].astype(int) 
        df['created_on'] = pd.to_datetime(df['created_on'], errors='coerce')
        df['invoiceitem_id'] = df['invoiceitem_id'].astype(int)
        df['invoiceitem_name'] = df['invoiceitem_name'].astype(str)
        df['type'] = df['type'].astype(str)
        df['unit_price'] = df['unit_price'].astype(int)
        df['total_price'] = df['total_price'].astype(int)
        df['percentage_in_invoice'] = df['percentage_in_invoice'].astype(float)
        df['is_expired'] = df['is_expired'].astype(bool)
        
        df = df.sort_values(by=['invoice_id', 'invoiceitem_id']).reset_index(drop=True)
        
        return df

    def save_to_csv(self, df, output_file_path):
        df.to_csv(output_file_path, index=False)


# File paths
invoices_path = '/Users/meryasatryan/Desktop/ServiceTitan-Data-Science-Internship/data/invoices_new.pkl'
expired_invoices_path = '/Users/meryasatryan/Desktop/ServiceTitan-Data-Science-Internship/data/expired_invoices.txt'
output_csv_path = '/Users/meryasatryan/Desktop/ServiceTitan-Data-Science-Internship/structured_invoices.csv'


extractor = InvoiceDataExtractor(invoices_path, expired_invoices_path)
extractor.load_invoices()
transformed_data = extractor.transform_invoices()
extractor.save_to_csv(transformed_data, output_csv_path)
print(transformed_data.head())
