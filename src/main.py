import typing
import psycopg2
import psycopg2.extras
from datetime import date
import apache_beam as beam
from decimal import Decimal

class DateCoder(beam.coders.Coder):
	def encode(self, d):
		return d.isoformat().encode('ascii')
	def decode(self, bs):
		return date.fromisoformat(bs.decode('ascii'))
	def is_deterministic(self):
		return True
beam.coders.registry.register_coder(date, DateCoder)

def db_connect():
	db_connection = psycopg2.connect(
		host='db',
		database='database',
		user='user',
		password='password',
		port=5432,
	)
	db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	return db_connection, db_cursor

class Product(typing.NamedTuple):
	display_name: str
	price: Decimal

class Seller(typing.NamedTuple):
	display_name: str
	daily_revenue_target: typing.Optional[Decimal]

class SellerSummary(typing.NamedTuple):
	seller_id: int
	date_day: date
	total_revenue: Decimal

class Transaction(typing.NamedTuple):
	date_day: date
	product_id: int
	seller_id: int
	units_sold: int

class Err(typing.NamedTuple):
	error_message: str

class ProcessErrors(beam.DoFn):
	def process(self, record):
		if isinstance(record, Err):
			yield beam.pvalue.TaggedOutput('invalid', record)
		else:
			yield record

def construct_pipeline(pipeline):
	'''
		This pipeline:
		- reads rows from a csv
		- parses them into the Transaction type, splitting parsing errors into a separate collection
		- ensures each Transaction references a Seller and Product that exist in the database, splitting invalid Transactions into a separate collection
		- aggregates the transactions into SellarSummary types

		The function returns pcollections directly.
	'''

	def parse_transaction(transaction_row):
		elements = transaction_row.split(',')
		if len(elements) != 4:
			return Err(f"invalid number of csv columns: {transaction_row}")

		date_day, product_id, seller_id, units_sold = elements
		try:
			#### TODO parse ISO string date into date_day (there's probably a builtin function to do this...)
			date_day = date.fromisoformat(date_day)
			product_id = int(product_id)
			seller_id = int(seller_id)
			units_sold = int(units_sold)
		except Exception as e:
			return Err(f"invalid transaction types for {transaction_row}: {e}")
		return Transaction(date_day, product_id, seller_id, units_sold)

	parsed_transactions, invalid_lines = (
		pipeline
		| 'read csv rows' >> beam.io.ReadFromText('./data/transactions.csv')
		| 'attempt to parse' >> beam.Map(parse_transaction)
		# TODO need to find some way to split Err rows from correct ones in beam
		| 'split parse errors' >> beam.ParDo(ProcessErrors()).with_outputs('invalid', main='parsed_transactions')
	)

	# print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Parsed Transactions $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
	# parsed_transactions | 'print parsed' >> beam.Map(print)

	def query_products_sellers():
		_db_connection, db_cursor = db_connect()

		db_cursor.execute("select id, display_name, daily_revenue_target from seller")
		for row in db_cursor:
			yield beam.pvalue.TaggedOutput('sellers_map', (row['id'], Seller(row['display_name'], row['daily_revenue_target'])))

		db_cursor.execute("select id, display_name, price from product")
		yield from map(lambda row: (row['id'], Product(row['display_name'], row['price'])), db_cursor)


	products_sellers_collection = (
		pipeline
		| 'singleton collection to start' >> beam.Create([tuple()])
		| 'gather products and sellers' >> beam.FlatMapTuple(query_products_sellers).with_outputs()
	)
	sellers_map = beam.pvalue.AsDict(products_sellers_collection.sellers_map)
	products_collection = products_sellers_collection[None]

	# print("$$$$$$$$$$$$$$$$$ Products Sellers Collection ###################")
	# products_sellers_collection | "count" >> beam.combiners.Count.Globally() | beam.Map(print)

	# print("$$$$$$$$$$$$$$$ Seller Map #########################")
	# # sellers_map | "print sm" >> beam.Map(print)
	# sellers_map | "count" >> beam.combiners.Count.Globally() | beam.Map(print)


	# print("$$$$$$$$$$$$$$$$$$$ P Collection $$$$$$$$$$$$$$$$$$$$$$$")
	# # This works
	# products_collection | "print p collection" >> beam.Map(print)

	def validate_transaction_references(product_id, join_results, sellers_map=None):
		transactions = join_results['transactions']
		products = list(join_results['products'])
		if len(products) > 1:
			raise ValueError("non-unique products! (this shouldn't happen)")

		if len(products) == 0:
			for transaction in transactions:
				yield Err(f"transaction references invalid product: {transaction}")
			return

		product = products[0]
		for transaction in transactions:
			# now also check that the seller is valid
			seller = sellers_map.get(transaction.seller_id, None)
			if seller == None:
				yield Err(f"transaction references invalid seller: {transaction}")
			else:
				# otherwise it's valid
				yield (transaction, product)

	validated_transactions_with_product, invalid_transactions = (
		({
			'transactions': (
				parsed_transactions
				| 'key transactions by product_id' >> beam.Map(lambda transaction: (transaction.product_id, transaction))
			),
			'products': products_collection,
		})
		| 'join by product_id' >> beam.CoGroupByKey()
		| 'merge and validate transaction references' >> beam.FlatMapTuple(
			validate_transaction_references,
			sellers_map=sellers_map,
		)
		# TODO same as above, need to split Err from correct, possibly even using the same function?
		# | 'print' >> beam.Map(print)
		| 'split validation errors' >> beam.ParDo(ProcessErrors()).with_outputs('invalid', main='validated_transactions_with_product')
		# | 'print' >> beam.Map(print)

	)

	# print("$$$$$$$$$$$$$$$$$$$ Validated transactions with product ##################")
	# validated_transactions_with_product | "print vtwp" >> beam.Map(print)

	# print("$$$$$$$$$$$$$$$$$$$$$$$$ invalid trans $$$$$$$$$$$$$$$$$$$$")
	# invalid_transactions | "print" >> beam.Map(print)

	seller_summary_days = (
		validated_transactions_with_product
		| 'key by seller_id and day' >> beam.MapTuple(lambda transaction, product: (
			(transaction.seller_id, transaction.date_day),
			product.price * transaction.units_sold,
		)).with_output_types(typing.Tuple[typing.Tuple[int, date], float])
		| 'aggregate' >> beam.CombinePerKey(sum)
		| 'make seller summaries' >> beam.MapTuple(lambda seller_date, total_revenue: SellerSummary(*seller_date, total_revenue))
	)

	validated_transactions = (
		validated_transactions_with_product
		# TODO need to change these rows of (transaction, product) into just transaction
		# | 'print' >> beam.Map(print)
		| 'remove products' >> beam.Map(lambda record: record[0])
		# | 'print' >> beam.Map(print)
	)

	print("$$$$$$$$$$$$$$$$$$$$ validated transaction $$$$$$$$$$$$$$$$$$$$$$$")
	validated_transactions | "print vt" >> beam.Map(print)

	return validated_transactions, seller_summary_days, invalid_lines, invalid_transactions


class SellerSuccessfulDays(typing.NamedTuple):
	seller_id: int
	display_name: str
	days: typing.List[date]

def get_successful_days():
	_db_connection, db_cursor = db_connect()

	# TODO need to write a query that gets data in the same shape as SellerSuccessfulDays
	# a SellerSuccessfulDay is all the days where a seller successfully sold more revenue than their daily_revenue_target
	# so only one SellerSuccessfulDays should be returned per seller
	# however not all sellers have a daily_revenue_target set, so just ignore them
	# the date objects in the days list should be sorted in ascending order
	# I know that postgres supports array values...
	db_cursor.execute('''
		select ... from ...
	''')

	return [SellerSuccessfulDays(**row) for row in db_cursor]
