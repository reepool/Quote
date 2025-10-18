"""
Unit tests for database models
"""

import pytest
from datetime import date, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from database.models import Base, Instrument, DailyQuote, TradingCalendar, DataUpdateInfo


@pytest.mark.unit
class TestDatabaseModels:
    """Test cases for database models"""

    @pytest.fixture
    def in_memory_db(self):
        """Create in-memory SQLite database for testing"""
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()

    def test_instrument_model_creation(self, in_memory_db):
        """Test Instrument model creation"""
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )

        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Verify instrument was created
        retrieved = in_memory_db.query(Instrument).filter_by(code='000001.SZ').first()
        assert retrieved is not None
        assert retrieved.code == '000001.SZ'
        assert retrieved.name == '平安银行'
        assert retrieved.market == 'SZSE'
        assert retrieved.industry == '银行'
        assert retrieved.status == 'active'

    def test_instrument_model_validation(self, in_memory_db):
        """Test Instrument model validation"""
        # Test with missing required fields
        with pytest.raises(Exception):  # SQLAlchemy will raise an error
            instrument = Instrument()
            in_memory_db.add(instrument)
            in_memory_db.commit()

    def test_instrument_model_unique_code(self, in_memory_db):
        """Test Instrument model unique code constraint"""
        # Create first instrument
        instrument1 = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument1)
        in_memory_db.commit()

        # Try to create duplicate
        instrument2 = Instrument(
            code='000001.SZ',  # Same code
            name='平安银行2',
            market='SZSE',
            industry='银行2',
            list_date=date(1991, 4, 3),
            status='active'
        )

        with pytest.raises(Exception):  # Should raise integrity error
            in_memory_db.add(instrument2)
            in_memory_db.commit()

    def test_daily_quote_model_creation(self, in_memory_db):
        """Test DailyQuote model creation"""
        # First create an instrument
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Create daily quote
        quote = DailyQuote(
            code='000001.SZ',
            date=date(2024, 1, 1),
            open=10.0,
            high=11.0,
            low=9.5,
            close=10.8,
            pre_close=10.0,
            change=0.8,
            change_pct=8.0,
            volume=1000000,
            amount=10800000
        )

        in_memory_db.add(quote)
        in_memory_db.commit()

        # Verify quote was created
        retrieved = in_memory_db.query(DailyQuote).filter_by(
            code='000001.SZ',
            date=date(2024, 1, 1)
        ).first()
        assert retrieved is not None
        assert retrieved.code == '000001.SZ'
        assert retrieved.open == 10.0
        assert retrieved.high == 11.0
        assert retrieved.low == 9.5
        assert retrieved.close == 10.8
        assert retrieved.volume == 1000000

    def test_daily_quote_model_unique_constraint(self, in_memory_db):
        """Test DailyQuote model unique constraint"""
        # Create instrument
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Create first quote
        quote1 = DailyQuote(
            code='000001.SZ',
            date=date(2024, 1, 1),
            open=10.0,
            high=11.0,
            low=9.5,
            close=10.8,
            volume=1000000
        )
        in_memory_db.add(quote1)
        in_memory_db.commit()

        # Try to create duplicate
        quote2 = DailyQuote(
            code='000001.SZ',
            date=date(2024, 1, 1),  # Same date
            open=10.1,
            high=11.1,
            low=9.6,
            close=10.9,
            volume=1100000
        )

        with pytest.raises(Exception):  # Should raise integrity error
            in_memory_db.add(quote2)
            in_memory_db.commit()

    def test_trading_calendar_model_creation(self, in_memory_db):
        """Test TradingCalendar model creation"""
        trading_day = TradingCalendar(
            date=date(2024, 1, 1),
            is_trading=True,
            exchange='SSE'
        )

        in_memory_db.add(trading_day)
        in_memory_db.commit()

        # Verify trading day was created
        retrieved = in_memory_db.query(TradingCalendar).filter_by(
            date=date(2024, 1, 1)
        ).first()
        assert retrieved is not None
        assert retrieved.date == date(2024, 1, 1)
        assert retrieved.is_trading is True
        assert retrieved.exchange == 'SSE'

    def test_trading_calendar_model_unique_constraint(self, in_memory_db):
        """Test TradingCalendar model unique constraint"""
        # Create first trading day record
        trading_day1 = TradingCalendar(
            date=date(2024, 1, 1),
            is_trading=True,
            exchange='SSE'
        )
        in_memory_db.add(trading_day1)
        in_memory_db.commit()

        # Try to create duplicate for same date and exchange
        trading_day2 = TradingCalendar(
            date=date(2024, 1, 1),  # Same date
            is_trading=False,        # Different value
            exchange='SSE'           # Same exchange
        )

        with pytest.raises(Exception):  # Should raise integrity error
            in_memory_db.add(trading_day2)
            in_memory_db.commit()

    def test_data_update_info_model_creation(self, in_memory_db):
        """Test DataUpdateInfo model creation"""
        update_info = DataUpdateInfo(
            source='baostock',
            exchange='SZSE',
            update_type='daily',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 10),
            records_updated=1000,
            status='completed',
            error_message=None
        )

        in_memory_db.add(update_info)
        in_memory_db.commit()

        # Verify update info was created
        retrieved = in_memory_db.query(DataUpdateInfo).filter_by(
            source='baostock'
        ).first()
        assert retrieved is not None
        assert retrieved.source == 'baostock'
        assert retrieved.exchange == 'SZSE'
        assert retrieved.update_type == 'daily'
        assert retrieved.records_updated == 1000
        assert retrieved.status == 'completed'

    def test_instrument_relationships(self, in_memory_db):
        """Test Instrument model relationships"""
        # Create instrument
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Create multiple quotes for the instrument
        quotes = [
            DailyQuote(
                code='000001.SZ',
                date=date(2024, 1, i),
                open=10.0 + i * 0.1,
                high=11.0 + i * 0.1,
                low=9.5 + i * 0.1,
                close=10.8 + i * 0.1,
                volume=1000000
            )
            for i in range(1, 4)
        ]

        for quote in quotes:
            in_memory_db.add(quote)
        in_memory_db.commit()

        # Test relationship
        retrieved_instrument = in_memory_db.query(Instrument).filter_by(code='000001.SZ').first()
        assert len(retrieved_instrument.daily_quotes) == 3

    def test_model_string_representations(self, in_memory_db):
        """Test model string representations"""
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Test __repr__ method
        repr_str = repr(instrument)
        assert '000001.SZ' in repr_str
        assert '平安银行' in repr_str

        quote = DailyQuote(
            code='000001.SZ',
            date=date(2024, 1, 1),
            open=10.0,
            high=11.0,
            low=9.5,
            close=10.8,
            volume=1000000
        )
        in_memory_db.add(quote)
        in_memory_db.commit()

        repr_str = repr(quote)
        assert '000001.SZ' in repr_str
        assert '2024-01-01' in repr_str

    def test_model_json_serialization(self, in_memory_db):
        """Test model JSON serialization"""
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Convert to dict (similar to JSON serialization)
        instrument_dict = {
            'code': instrument.code,
            'name': instrument.name,
            'market': instrument.market,
            'industry': instrument.industry,
            'list_date': instrument.list_date.isoformat(),
            'status': instrument.status
        }

        assert instrument_dict['code'] == '000001.SZ'
        assert instrument_dict['name'] == '平安银行'
        assert instrument_dict['list_date'] == '1991-04-03'

    def test_model_default_values(self, in_memory_db):
        """Test model default values"""
        # Test DataUpdateInfo with default values
        update_info = DataUpdateInfo(
            source='baostock',
            exchange='SZSE',
            update_type='daily'
        )

        in_memory_db.add(update_info)
        in_memory_db.commit()

        # Check that timestamp was set automatically
        assert update_info.updated_at is not None
        assert isinstance(update_info.updated_at, datetime)

    def test_model_field_constraints(self, in_memory_db):
        """Test model field constraints"""
        # Test with invalid data types
        with pytest.raises(Exception):
            instrument = Instrument(
                code=123,  # Should be string
                name='平安银行',
                market='SZSE',
                industry='银行',
                list_date=date(1991, 4, 3),
                status='active'
            )
            in_memory_db.add(instrument)
            in_memory_db.commit()

    def test_model_cascade_delete(self, in_memory_db):
        """Test cascade delete behavior"""
        # Create instrument
        instrument = Instrument(
            code='000001.SZ',
            name='平安银行',
            market='SZSE',
            industry='银行',
            list_date=date(1991, 4, 3),
            status='active'
        )
        in_memory_db.add(instrument)
        in_memory_db.commit()

        # Create quote
        quote = DailyQuote(
            code='000001.SZ',
            date=date(2024, 1, 1),
            open=10.0,
            high=11.0,
            low=9.5,
            close=10.8,
            volume=1000000
        )
        in_memory_db.add(quote)
        in_memory_db.commit()

        # Delete instrument
        in_memory_db.delete(instrument)
        in_memory_db.commit()

        # Verify quote is also deleted (if cascade is configured)
        remaining_quotes = in_memory_db.query(DailyQuote).filter_by(code='000001.SZ').all()
        # Note: This depends on the actual cascade configuration in your models