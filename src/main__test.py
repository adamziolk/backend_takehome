from datetime import date
import apache_beam as beam
from decimal import Decimal
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException

import main
from main import Product, Seller, SellerSummary, Transaction, Err, SellerSuccessfulDays

def test__construct_pipeline():
	with TestPipeline() as pipeline:
		validated_transactions, seller_summary_days, invalid_lines, invalid_transactions = \
			main.construct_pipeline(pipeline)

		# validated_transactions | 'print validated_transactions' >> beam.Map(print)
		assert_that(validated_transactions, equal_to([
			Transaction(date_day=date(2021, 1, 5), product_id=12, seller_id=4, units_sold=38),
			Transaction(date_day=date(2021, 1, 5), product_id=12, seller_id=3, units_sold=82),
			Transaction(date_day=date(2021, 1, 5), product_id=5, seller_id=2, units_sold=5),
			Transaction(date_day=date(2021, 1, 4), product_id=5, seller_id=3, units_sold=36),
			Transaction(date_day=date(2021, 1, 3), product_id=5, seller_id=1, units_sold=64),
			Transaction(date_day=date(2021, 1, 4), product_id=5, seller_id=4, units_sold=12),
			Transaction(date_day=date(2021, 1, 4), product_id=2, seller_id=4, units_sold=9),
			Transaction(date_day=date(2021, 1, 5), product_id=2, seller_id=3, units_sold=68),
			Transaction(date_day=date(2021, 1, 5), product_id=2, seller_id=1, units_sold=52),
			Transaction(date_day=date(2021, 1, 4), product_id=2, seller_id=3, units_sold=43),
			Transaction(date_day=date(2021, 1, 4), product_id=2, seller_id=1, units_sold=75),
			Transaction(date_day=date(2021, 1, 4), product_id=2, seller_id=4, units_sold=81),
			Transaction(date_day=date(2021, 1, 4), product_id=1, seller_id=1, units_sold=52),
			Transaction(date_day=date(2021, 1, 4), product_id=1, seller_id=4, units_sold=48),
			Transaction(date_day=date(2021, 1, 5), product_id=6, seller_id=4, units_sold=43),
			Transaction(date_day=date(2021, 1, 4), product_id=6, seller_id=3, units_sold=38),
			Transaction(date_day=date(2021, 1, 4), product_id=6, seller_id=2, units_sold=64),
			Transaction(date_day=date(2021, 1, 5), product_id=6, seller_id=2, units_sold=43),
			Transaction(date_day=date(2021, 1, 4), product_id=4, seller_id=3, units_sold=99),
			Transaction(date_day=date(2021, 1, 4), product_id=4, seller_id=4, units_sold=77),
			Transaction(date_day=date(2021, 1, 4), product_id=4, seller_id=4, units_sold=92),
			Transaction(date_day=date(2021, 1, 5), product_id=8, seller_id=3, units_sold=72),
			Transaction(date_day=date(2021, 1, 4), product_id=8, seller_id=3, units_sold=92),
			Transaction(date_day=date(2021, 1, 4), product_id=8, seller_id=2, units_sold=73),
			Transaction(date_day=date(2021, 1, 3), product_id=7, seller_id=3, units_sold=56),
			Transaction(date_day=date(2021, 1, 4), product_id=7, seller_id=1, units_sold=28),
			Transaction(date_day=date(2021, 1, 4), product_id=7, seller_id=3, units_sold=39),
			Transaction(date_day=date(2021, 1, 4), product_id=9, seller_id=3, units_sold=82),
			Transaction(date_day=date(2021, 1, 3), product_id=9, seller_id=2, units_sold=47),
			Transaction(date_day=date(2021, 1, 4), product_id=9, seller_id=3, units_sold=86),
			Transaction(date_day=date(2021, 1, 4), product_id=9, seller_id=3, units_sold=99),
			Transaction(date_day=date(2021, 1, 5), product_id=11, seller_id=3, units_sold=35),
			Transaction(date_day=date(2021, 1, 4), product_id=11, seller_id=2, units_sold=97),
			Transaction(date_day=date(2021, 1, 3), product_id=11, seller_id=3, units_sold=80),
			Transaction(date_day=date(2021, 1, 4), product_id=11, seller_id=2, units_sold=50),
			Transaction(date_day=date(2021, 1, 5), product_id=10, seller_id=2, units_sold=70),
			Transaction(date_day=date(2021, 1, 4), product_id=10, seller_id=1, units_sold=49),
			Transaction(date_day=date(2021, 1, 4), product_id=3, seller_id=3, units_sold=14),
		]), label='assert validated_transactions')

		# seller_summary_days | 'print seller_summary_days' >> beam.Map(print)
		assert_that(seller_summary_days, equal_to([
			SellerSummary(seller_id=4, date_day=date(2021, 1, 5), total_revenue=Decimal('252.00')),
			SellerSummary(seller_id=3, date_day=date(2021, 1, 5), total_revenue=Decimal('804.75')),
			SellerSummary(seller_id=2, date_day=date(2021, 1, 5), total_revenue=Decimal('473.00')),
			SellerSummary(seller_id=3, date_day=date(2021, 1, 4), total_revenue=Decimal('2040.50')),
			SellerSummary(seller_id=1, date_day=date(2021, 1, 3), total_revenue=Decimal('128.00')),
			SellerSummary(seller_id=4, date_day=date(2021, 1, 4), total_revenue=Decimal('1101.50')),
			SellerSummary(seller_id=1, date_day=date(2021, 1, 5), total_revenue=Decimal('156.00')),
			SellerSummary(seller_id=1, date_day=date(2021, 1, 4), total_revenue=Decimal('781.00')),
			SellerSummary(seller_id=2, date_day=date(2021, 1, 4), total_revenue=Decimal('578.25')),
			SellerSummary(seller_id=3, date_day=date(2021, 1, 3), total_revenue=Decimal('316.00')),
			SellerSummary(seller_id=2, date_day=date(2021, 1, 3), total_revenue=Decimal('235.00')),
		]), label='assert seller_summary_days')

		# invalid_lines | 'print invalid_lines' >> beam.Map(print)
		assert_that(invalid_lines, equal_to([
			Err(error_message="invalid transaction types for 2021-01-05,1,Han,35: invalid literal for int() with base 10: 'Han'"),
			Err(error_message="invalid transaction types for 20210103,3,2,8: Invalid isoformat string: '20210103'"),
			Err(error_message="invalid transaction types for 2021-01-05,10,2,$49.44: invalid literal for int() with base 10: '$49.44'"),
			Err(error_message='invalid number of csv columns: 2021-01-05,5.4.55'),
			Err(error_message="invalid transaction types for 01/04/2021,2,3,75: Invalid isoformat string: '01/04/2021'"),
			Err(error_message="invalid transaction types for 2021-01-05,Apple,2,31: invalid literal for int() with base 10: 'Apple'"),
			Err(error_message="invalid transaction types for 01/04/2021,1,4,70: Invalid isoformat string: '01/04/2021'"),
			Err(error_message='invalid number of csv columns: 2021-01-05,2.2.43'),
			Err(error_message="invalid transaction types for 2021-01-05,1,3,$54.52: invalid literal for int() with base 10: '$54.52'"),
		]), label='assert invalid_lines')

		# invalid_transactions | 'print invalid_transactions' >> beam.Map(print)
		assert_that(invalid_transactions, equal_to([
			Err(error_message='transaction references invalid seller: Transaction(date_day=datetime.date(2021, 1, 4), product_id=5, seller_id=20, units_sold=32)'),
			Err(error_message='transaction references invalid seller: Transaction(date_day=datetime.date(2021, 1, 5), product_id=6, seller_id=20, units_sold=16)'),
			Err(error_message='transaction references invalid product: Transaction(date_day=datetime.date(2021, 1, 3), product_id=110, seller_id=3, units_sold=10)'),
		]), label='assert invalid_transactions')


def test__get_successful_days():
	successful_days = main.get_successful_days()
	assert successful_days == [
		SellerSuccessfulDays(seller_id=1, display_name='Luke', days=[date(2021, 1, 2)]),
		SellerSuccessfulDays(seller_id=2, display_name='Leia', days=[date(2021, 1, 1), date(2021, 1, 2)]),
		SellerSuccessfulDays(seller_id=4, display_name='Chewie', days=[date(2021, 1, 2)]),
	]
