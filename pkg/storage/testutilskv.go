package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// KVTestSuite is a generic test suite for KV implementations
func KVTestSuite(t *testing.T, kv KV) { //nolint:funlen,gocognit,gocyclo // it's a test suite
	t.Run("Nil key operations", func(t *testing.T) {
		// given
		var nilKey []byte
		value := []byte("some value")

		// Test Set with nil key
		t.Run("Set with nil key", func(t *testing.T) {
			// when
			_, err := kv.Set(nilKey, value)

			// then
			assert.Error(t, err, "Set should return error for nil key")
		})

		// Test Get with nil key
		t.Run("Get with nil key", func(t *testing.T) {
			// when
			_, err := kv.Get(nilKey)

			// then
			assert.Error(t, err, "Get should return error for nil key")
		})

		// Test Delete with nil key
		t.Run("Delete with nil key", func(t *testing.T) {
			// when
			err := kv.Delete(nilKey)

			// then
			assert.Error(t, err, "Delete should return error for nil key")
		})
	})

	t.Run("Empty key operations", func(t *testing.T) {
		// given
		emptyKey := []byte{}
		value := []byte("some value")

		// Test Set with empty key
		t.Run("Set with empty key", func(t *testing.T) {
			// when
			_, err := kv.Set(emptyKey, value)

			// then
			assert.Error(t, err, "Set should return error for empty key")
		})

		// Test Get with empty key
		t.Run("Get with empty key", func(t *testing.T) {
			// when
			_, err := kv.Get(emptyKey)

			// then
			assert.Error(t, err, "Get should return error for empty key")
		})

		// Test Delete with empty key
		t.Run("Delete with empty key", func(t *testing.T) {
			// when
			err := kv.Delete(emptyKey)

			// then
			assert.Error(t, err, "Delete should return error for empty key")
		})
	})

	t.Run("Nil value operations", func(t *testing.T) {
		// given
		key := []byte("test-key")
		var nilValue []byte

		// Test Set with nil value
		t.Run("Set with nil value", func(t *testing.T) {
			// when
			_, err := kv.Set(key, nilValue)

			// then
			assert.Error(t, err, "Set should return error for nil value")
		})
	})

	t.Run("Empty value operations", func(t *testing.T) {
		// given
		key := []byte("test-key")
		emptyValue := []byte{}

		// Test Set with empty value
		t.Run("Set with empty value", func(t *testing.T) {
			// when
			_, err := kv.Set(key, emptyValue)

			// then
			assert.NoError(t, err, "Set should accept empty value")

			// when - get the value
			got, err := kv.Get(key)

			// then
			assert.NoError(t, err)
			assert.Empty(t, got, "Get should return empty value")
		})
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		// given
		nonExistentKey := []byte("nonexistent")

		// when
		val, err := kv.Get(nonExistentKey)

		// then
		assert.NoError(t, err)
		assert.Nil(t, val)
	})

	t.Run("Set and Get", func(t *testing.T) {
		// given
		testCases := []struct {
			name  string
			key   []byte
			value []byte
		}{
			{
				name:  "basic key-value pair",
				key:   []byte("testkey"),
				value: []byte("testvalue"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// when
				_, err := kv.Set(tc.key, tc.value)

				// then
				assert.NoError(t, err)

				// when
				got, err := kv.Get(tc.key)

				// then
				assert.NoError(t, err)
				assert.Equal(t, tc.value, got)
			})
		}
	})

	t.Run("Key overwriting behavior", func(t *testing.T) {
		// given
		key := []byte("overwrite-key")
		firstValue := []byte("first value")
		secondValue := []byte("second value")

		// when - set the first value
		_, err := kv.Set(key, firstValue)

		// then
		assert.NoError(t, err)

		// when - get the first value to verify it was set
		got, err := kv.Get(key)

		// then
		assert.NoError(t, err)
		assert.Equal(t, firstValue, got)

		// when - overwrite with second value
		_, err = kv.Set(key, secondValue)

		// then
		assert.NoError(t, err)

		// when - get the value again
		got, err = kv.Get(key)

		// then
		assert.NoError(t, err)
		assert.Equal(t, secondValue, got, "Value should be overwritten when Set is called with the same key")
	})

	t.Run("Binary data and special characters", func(t *testing.T) {
		// given
		testCases := []struct {
			name  string
			key   []byte
			value []byte
		}{
			{
				name:  "binary key",
				key:   []byte{0x00, 0x01, 0x02, 0x03, 0xFF},
				value: []byte("value for binary key"),
			},
			{
				name:  "binary value",
				key:   []byte("key for binary value"),
				value: []byte{0x00, 0x01, 0x02, 0x03, 0xFF},
			},
			{
				name:  "special characters in key",
				key:   []byte("!@#$%^&*()_+{}|:<>?"),
				value: []byte("value for special chars"),
			},
			{
				name:  "unicode characters in key",
				key:   []byte("résumé-üñïçødê-😀"),
				value: []byte("value for unicode"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// when - set the value
				_, err := kv.Set(tc.key, tc.value)

				// then
				assert.NoError(t, err)

				// when - get the value
				got, err := kv.Get(tc.key)

				// then
				assert.NoError(t, err)
				assert.Equal(t, tc.value, got)
			})
		}
	})

	t.Run("Delete non-existent key", func(t *testing.T) {
		// given
		nonExistentKey := []byte("nonexistent")

		// when
		err := kv.Delete(nonExistentKey)

		// then
		assert.NoError(t, err)
	})

	t.Run("Set, Delete, and Get", func(t *testing.T) {
		// given
		testCases := []struct {
			name  string
			key   []byte
			value []byte
		}{
			{
				name:  "delete existing key",
				key:   []byte("testkey"),
				value: []byte("testvalue"),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// given - set up the value
				_, err := kv.Set(tc.key, tc.value)
				assert.NoError(t, err)

				// when - delete the value
				err = kv.Delete(tc.key)

				// then
				assert.NoError(t, err)

				// when - try to get the deleted value
				got, err := kv.Get(tc.key)

				// then
				assert.NoError(t, err)
				assert.Nil(t, got)
			})
		}
	})

	t.Run("Set Options", func(t *testing.T) {
		t.Run("WithGet option", func(t *testing.T) {
			// given
			key := []byte("get-option-key")
			value := []byte("original-value")
			newValue := []byte("new-value")

			// Cleanup first
			err := kv.Delete(key)
			assert.NoError(t, err)

			// Test setting a new key with Get option
			t.Run("set new key with Get option", func(t *testing.T) {
				// when - set a new key with Get option
				returnedValue, err := kv.Set(key, value, WithReturnPreviousValue(true))

				// then
				assert.NoError(t, err)
				assert.Nil(t, returnedValue, "Should return nil when setting a new key")

				// Verify the value was set
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, value, got)
			})

			// Test overwriting a key with Get option
			t.Run("overwrite key with Get option", func(t *testing.T) {
				// when - overwrite the key with Get option
				returnedValue, err := kv.Set(key, newValue, WithReturnPreviousValue(true))

				// then
				assert.NoError(t, err)
				assert.Equal(t, value, returnedValue, "Should return the previous value")

				// Verify the value was updated
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, newValue, got)
			})

			// Cleanup
			err = kv.Delete(key)
			assert.NoError(t, err)
		})

		t.Run("WithSkipIfKeyAlreadyExists option", func(t *testing.T) {
			// given
			key := []byte("skip-if-exists-key")
			originalValue := []byte("original-value")
			newValue := []byte("new-value")

			// Cleanup first
			err := kv.Delete(key)
			assert.NoError(t, err)

			// Test setting a new key with SkipIfKeyAlreadyExists option
			t.Run("set new key with SkipIfKeyAlreadyExists", func(t *testing.T) {
				// when - set a new key with SkipIfKeyAlreadyExists
				returnedValue, err := kv.Set(key, originalValue, WithSkipIfKeyAlreadyExists(true))

				// then
				assert.NoError(t, err)
				assert.Nil(t, returnedValue, "Should return nil for a new key")

				// Verify the value was set
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, originalValue, got)
			})

			// Test trying to overwrite an existing key with SkipIfKeyAlreadyExists
			t.Run("try to overwrite with SkipIfKeyAlreadyExists", func(t *testing.T) {
				// when - try to overwrite with SkipIfKeyAlreadyExists
				returnedValue, err := kv.Set(key, newValue, WithSkipIfKeyAlreadyExists(true))

				// then
				assert.NoError(t, err)
				assert.Nil(t, returnedValue, "Should not return a value even if key exists")

				// Verify the value was NOT updated
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, originalValue, got, "Value should not be changed")
			})

			// Cleanup
			err = kv.Delete(key)
			assert.NoError(t, err)
		})

		t.Run("Combined options", func(t *testing.T) {
			// given
			key := []byte("combined-options-key")
			originalValue := []byte("original-value")
			newValue := []byte("new-value")

			// Cleanup first
			err := kv.Delete(key)
			assert.NoError(t, err)

			// Test setting a new key with both options
			t.Run("set new key with both options", func(t *testing.T) {
				// when - set a new key with both options
				returnedValue, err := kv.Set(key, originalValue, WithSkipIfKeyAlreadyExists(true), WithReturnPreviousValue(true))

				// then
				assert.NoError(t, err)
				assert.Nil(t, returnedValue, "Should return nil for a new key")

				// Verify the value was set
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, originalValue, got)
			})

			// Test trying to overwrite an existing key with both options
			t.Run("try to overwrite with both options", func(t *testing.T) {
				// when - try to overwrite with both options
				returnedValue, err := kv.Set(key, newValue, WithSkipIfKeyAlreadyExists(true), WithReturnPreviousValue(true))

				// then
				assert.NoError(t, err)
				assert.Equal(t, originalValue, returnedValue, "Should return the current value when key exists")

				// Verify the value was NOT updated
				got, err := kv.Get(key)
				assert.NoError(t, err)
				assert.Equal(t, originalValue, got, "Value should not be changed")
			})

			// Clean up
			err = kv.Delete(key)
			assert.NoError(t, err)
		})
	})
}
