// Package currency provides a currency conversion interface and implementation.
package currency

// ISOCurrencyUSD is the ISO-4217 currency code for USD.
const ISOCurrencyUSD = "USD"

// Converter is an interface that converts currency amounts between different ISO currencies.
type Converter interface {
	Convert(isoBaseCurrency string, isoQuoteCurrency string, amount float64) (float64, error)
}

type dummyConverter struct {
	factor float64
}

func (c *dummyConverter) Convert(_, _ string, amount float64) (float64, error) {
	return amount * c.factor, nil
}

// NewDummyConverter creates a new dummy currency converter.
func NewDummyConverter(factor float64) Converter {
	return &dummyConverter{factor: factor}
}

// DoConversion is a helper, that can be used in column implementations
// to quickly convert a value stored in other columns
func DoConversion(
	converter Converter,
	baseCurrency any, // Assumed to be a string
	quoteCurrency string, // Assumed to be a string
	baseValue any, // Assumed to be a float64
) (any, error) {
	if baseCurrency == nil || baseValue == nil {
		return nil, nil // nolint:nilnil // intentionally return nil, nil when input is nil
	}
	baseCurrencyStr, ok := baseCurrency.(string)
	if !ok {
		return nil, nil // nolint:nilnil // return nil, nil when baseCurrency is not a string
	}
	baseValueFloat, ok := baseValue.(float64)
	if !ok {
		return nil, nil // nolint:nilnil // return nil, nil when baseValue is not a float64
	}
	converted, err := converter.Convert(baseCurrencyStr, quoteCurrency, baseValueFloat)
	if err != nil {
		return nil, err
	}
	rounded := float64(int64(converted*100+0.5)) / 100
	return rounded, nil
}
