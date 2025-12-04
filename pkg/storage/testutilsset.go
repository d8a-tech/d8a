package storage

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// SetTestSuite is a generic test suite for Set implementations
func SetTestSuite(t *testing.T, set Set) { //nolint:funlen,gocognit // it's a test suite
	t.Run("Add and All", func(t *testing.T) {
		// given
		testCases := []struct {
			name    string
			key     []byte
			values  [][]byte
			allSeen bool
		}{
			{
				name: "multiple values for key",
				key:  []byte("testkey"),
				values: [][]byte{
					[]byte("value1"),
					[]byte("value2"),
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// when - add values
				for _, value := range tc.values {
					err := set.Add(tc.key, value)
					assert.NoError(t, err)
				}

				// when - get all values
				values, err := set.All(tc.key)

				// then
				assert.NoError(t, err)
				assert.Equal(t, len(tc.values), len(values), "All returned wrong number of values")

				// Check that all values are present
				for _, expectedVal := range tc.values {
					found := false
					for _, gotVal := range values {
						if bytes.Equal(gotVal, expectedVal) {
							found = true
							break
						}
					}
					assert.True(t, found, "Value %s not found in returned values", string(expectedVal))
				}
			})
		}
	})

	t.Run("All for non-existent key", func(t *testing.T) {
		// given
		nonExistentKey := []byte("nonexistent")

		// when
		values, err := set.All(nonExistentKey)

		// then
		assert.NoError(t, err)
		assert.Empty(t, values, "All returned non-empty slice for non-existent key")
	})

	t.Run("Nil and Empty Key/Value tests", func(t *testing.T) {
		// given
		testKey := []byte("testkey")
		testValue := []byte("testvalue")

		// when - nil key
		err := set.Add(nil, testValue)
		// then
		assert.Error(t, err, "Add should error on nil key")

		// when - nil value
		err = set.Add(testKey, nil)
		// then
		assert.Error(t, err, "Add should error on nil value")

		// when - empty key
		err = set.Add([]byte{}, testValue)
		// then
		assert.Error(t, err, "Add should error on empty key")

		// when - empty value (this should be allowed)
		err = set.Add(testKey, []byte{})
		// then
		assert.NoError(t, err, "Add should allow empty values")

		// Verify empty value was added
		values, err := set.All(testKey)
		assert.NoError(t, err)
		emptyValueFound := false
		for _, v := range values {
			if len(v) == 0 {
				emptyValueFound = true
				break
			}
		}
		assert.True(t, emptyValueFound, "Empty value should be stored and retrieved")

		// Clean up for next tests
		err = set.Drop(testKey)
		assert.NoError(t, err)
	})

	t.Run("Duplicate Values test", func(t *testing.T) {
		// given
		key := []byte("duplicatetest")
		value := []byte("duplicatevalue")

		// when - add the same value twice
		err := set.Add(key, value)
		assert.NoError(t, err)

		err = set.Add(key, value)
		assert.NoError(t, err)

		// then - should only store one instance of the value
		values, err := set.All(key)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(values), "Set should be idempotent and not store duplicates")

		// Clean up
		err = set.Drop(key)
		assert.NoError(t, err)
	})

	t.Run("Delete input validation", func(t *testing.T) {
		// given
		testCases := []struct {
			name  string
			key   []byte
			value []byte
		}{
			{
				name:  "nil key",
				value: []byte("value"),
			},
			{
				name:  "empty key",
				key:   []byte{},
				value: []byte("value"),
			},
			{
				name: "nil value",
				key:  []byte("key"),
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				// when
				err := set.Delete(tc.key, tc.value)

				// then
				assert.Error(t, err)
			})
		}
	})

	t.Run("Delete value operations", func(t *testing.T) {
		t.Run("delete specific value", func(t *testing.T) {
			// given
			key := []byte("delete-specific")
			values := [][]byte{[]byte("alpha"), []byte("beta")}
			for _, value := range values {
				err := set.Add(key, value)
				assert.NoError(t, err)
			}

			// when
			err := set.Delete(key, values[0])

			// then
			assert.NoError(t, err)

			// when
			got, err := set.All(key)

			// then
			assert.NoError(t, err)
			assert.Equal(t, 1, len(got))
			assert.True(t, bytes.Equal(values[1], got[0]))

			// cleanup
			err = set.Drop(key)
			assert.NoError(t, err)
		})

		t.Run("delete empty value", func(t *testing.T) {
			// given
			key := []byte("delete-empty")
			err := set.Add(key, []byte{})
			assert.NoError(t, err)

			// when
			err = set.Delete(key, []byte{})

			// then
			assert.NoError(t, err)

			// when
			got, err := set.All(key)

			// then
			assert.NoError(t, err)
			assert.Empty(t, got)

			// cleanup
			err = set.Drop(key)
			assert.NoError(t, err)
		})

		t.Run("delete non-existent value", func(t *testing.T) {
			// given
			key := []byte("delete-missing-value")
			err := set.Add(key, []byte("existing"))
			assert.NoError(t, err)

			// when
			err = set.Delete(key, []byte("missing"))

			// then
			assert.NoError(t, err)

			// when
			got, err := set.All(key)

			// then
			assert.NoError(t, err)
			assert.Len(t, got, 1)
			assert.True(t, bytes.Equal([]byte("existing"), got[0]))

			// cleanup
			err = set.Drop(key)
			assert.NoError(t, err)
		})

		t.Run("delete from missing key", func(t *testing.T) {
			// given
			key := []byte("missing-key")
			value := []byte("value")

			// when
			err := set.Delete(key, value)

			// then
			assert.NoError(t, err)
		})
	})

	t.Run("Drop", func(t *testing.T) {
		// given
		testCases := []struct {
			name  string
			key   []byte
			value []byte
		}{
			{
				name:  "delete existing set",
				key:   []byte("testkey"),
				value: []byte("testvalue"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// given - add a value
				err := set.Add(tc.key, tc.value)
				assert.NoError(t, err)

				// when - drop the key
				err = set.Drop(tc.key)

				// then
				assert.NoError(t, err)

				// when - check that the key is gone
				values, err := set.All(tc.key)

				// then
				assert.NoError(t, err)
				assert.Empty(t, values, "All returned non-empty slice after Drop")
			})
		}
	})

	t.Run("Drop non-existent key", func(t *testing.T) {
		// given
		nonExistentKey := []byte("nonexistent")

		// when
		err := set.Drop(nonExistentKey)

		// then
		assert.NoError(t, err)
	})

	t.Run("Concurrent Operations", func(t *testing.T) {
		// given
		key := []byte("concurrencytest")
		numGoroutines := 10
		numOperations := 100

		// when - concurrently add values
		var wg sync.WaitGroup
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < numOperations; i++ {
					value := []byte(string(rune(id*1000 + i)))
					err := set.Add(key, value)
					assert.NoError(t, err)
				}
			}(g)
		}
		wg.Wait()

		// then - check all expected values were added
		values, err := set.All(key)
		assert.NoError(t, err)

		// We expect uniqueness, so the total should be numGoroutines * numOperations
		// unless there are overlapping values (which shouldn't happen with our construction)
		assert.Equal(t, numGoroutines*numOperations, len(values),
			"Expected %d unique values after concurrent operations", numGoroutines*numOperations)

		// Check concurrent delete works
		err = set.Drop(key)
		assert.NoError(t, err)

		// Verify values are gone
		values, err = set.All(key)
		assert.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("Binary Data Values", func(t *testing.T) {
		// given
		key := []byte("binarydatakey")
		// Create values with non-printable bytes and null bytes
		binaryValues := [][]byte{
			{0x00, 0x01, 0x02, 0x03},                    // with null byte
			{0xFF, 0xFE, 0xFD, 0xFC},                    // high bytes
			{0x7F, 'h', 'e', 'l', 'l', 'o', 0x00, 0x1B}, // mix of ASCII and control chars
		}

		// when - add binary values
		for _, value := range binaryValues {
			err := set.Add(key, value)
			assert.NoError(t, err)
		}

		// then - retrieve and verify binary values
		values, err := set.All(key)
		assert.NoError(t, err)
		assert.Equal(t, len(binaryValues), len(values))

		// Verify each binary value was preserved exactly
		for _, expected := range binaryValues {
			found := false
			for _, got := range values {
				if bytes.Equal(expected, got) {
					found = true
					break
				}
			}
			assert.True(t, found, "Binary value not found or corrupted in storage")
		}

		// Clean up
		err = set.Drop(key)
		assert.NoError(t, err)
	})

	t.Run("Large Value (5k characters)", func(t *testing.T) {
		// given
		key := []byte("largevaluekey")
		// Create a 5000 character value
		largeValue := make([]byte, 5000)
		for i := 0; i < 5000; i++ {
			largeValue[i] = byte('A' + (i % 26)) // Fill with repeating A-Z pattern
		}

		// when - add large value
		err := set.Add(key, largeValue)

		// then
		assert.NoError(t, err)

		// when - retrieve the large value
		values, err := set.All(key)

		// then
		assert.NoError(t, err)
		assert.Equal(t, 1, len(values), "Should have exactly one large value")
		assert.True(t, bytes.Equal(largeValue, values[0]), "Large value should be preserved exactly")
		assert.Equal(t, 5000, len(values[0]), "Retrieved value should have correct length")

		// Clean up
		err = set.Drop(key)
		assert.NoError(t, err)
	})
}
