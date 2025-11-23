/// compact bitmap for tracking NULL values (validity)
/// uses 1 bit per value: 1 = valid, 0 = NULL
/// stored as u64 words for efficient operations
#[derive(Debug, Clone)]
pub struct Bitmap {
    /// data stored as u64 words (64 bits each)
    words: Vec<u64>,

    /// number of bits (number of values tracked)
    len: usize,
}

impl Bitmap {
    /// number of bits in a word
    const BITS_PER_WORD: usize = 64;

    /// create a new bitmap with all bits set to 1 (all valid)
    pub fn new(len: usize) -> Self {
        let num_words = (len + Self::BITS_PER_WORD - 1) / Self::BITS_PER_WORD;

        // initialize all words to u64::MAX (all bits = 1 = all valid)
        let words = vec![u64::MAX; num_words];

        Self { words, len }
    }

    /// create a new bitmap with all bits set to 0 (all NULL)
    pub fn new_all_null(len: usize) -> Self {
        let num_words = (len + Self::BITS_PER_WORD - 1) / Self::BITS_PER_WORD;
        let words = vec![0u64; num_words];

        Self { words, len }
    }

    /// get the word index and bit position for a given index
    #[inline]
    fn word_and_bit(index: usize) -> (usize, usize) {
        let word_index = index / Self::BITS_PER_WORD;
        let bit_index = index % Self::BITS_PER_WORD;
        (word_index, bit_index)
    }

    /// check if a value is valid (bit = 1)
    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        debug_assert!(index < self.len, "Index out of bounds");

        let (word_index, bit_index) = Self::word_and_bit(index);
        let word = self.words[word_index];
        (word & (1u64 << bit_index)) != 0
    }

    /// check if a value is NULL (bit = 0)
    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }

    /// set a bit to 1 (mark as valid)
    #[inline]
    pub fn set_valid(&mut self, index: usize) {
        debug_assert!(index < self.len, "Index out of bounds");

        let (word_index, bit_index) = Self::word_and_bit(index);
        self.words[word_index] |= 1u64 << bit_index;
    }

    /// set a bit to 0 (mark as NULL)
    #[inline]
    pub fn set_null(&mut self, index: usize) {
        debug_assert!(index < self.len, "Index out of bounds");

        let (word_index, bit_index) = Self::word_and_bit(index);
        self.words[word_index] &= !(1u64 << bit_index);
    }

    /// set a bit based on a boolean value
    #[inline]
    pub fn set(&mut self, index: usize, is_valid: bool) {
        if is_valid {
            self.set_valid(index);
        } else {
            self.set_null(index);
        }
    }

    /// check if all values are valid (all bits = 1)
    /// this is a fast path optimization - if all valid, we can skip NULL checks
    pub fn all_valid(&self) -> bool {
        // check all full words
        let full_words = self.len / Self::BITS_PER_WORD;
        for i in 0..full_words {
            if self.words[i] != u64::MAX {
                return false;
            }
        }

        // check remaining bits in last word
        let remaining_bits = self.len % Self::BITS_PER_WORD;
        if remaining_bits > 0 {
            let last_word_index = full_words;
            let mask = (1u64 << remaining_bits) - 1;
            if (self.words[last_word_index] & mask) != mask {
                return false;
            }
        }

        true
    }

    /// get the number of bits
    pub fn len(&self) -> usize {
        self.len
    }

    /// check if bitmap is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// clear all bits (set to 0 = all NULL)
    pub fn clear(&mut self) {
        for word in &mut self.words {
            *word = 0;
        }
    }

    /// reset to all valid (set all bits to 1)
    pub fn reset_all_valid(&mut self) {
        for word in &mut self.words {
            *word = u64::MAX;
        }
    }

    /// resize the bitmap, new bits are set to valid
    pub fn resize(&mut self, new_len: usize) {
        let new_num_words = (new_len + Self::BITS_PER_WORD - 1) / Self::BITS_PER_WORD;

        if new_num_words > self.words.len() {
            self.words.resize(new_num_words, u64::MAX);
        }

        self.len = new_len;
    }

    /// count the number of valid (non-NULL) values in the first `count` bits
    /// uses optimized algorithm with Kernighan's bit counting
    pub fn count_valid(&self, count: usize) -> usize {
        debug_assert!(count <= self.len, "Count exceeds bitmap length");

        if count == 0 {
            return 0;
        }

        // fast path: if all valid, just return count
        if self.all_valid() {
            return count;
        }

        let mut valid_count = 0;

        // process full words (64 bits at a time)
        let full_words = count / Self::BITS_PER_WORD;
        for i in 0..full_words {
            let word = self.words[i];

            // fast path: all bits valid in this word
            if word == u64::MAX {
                valid_count += Self::BITS_PER_WORD;
                continue;
            }

            // fast path: all bits NULL in this word
            if word == 0 {
                continue;
            }

            // partially valid: use Kernighan's algorithm to count set bits
            // this algorithm iterates once per set bit, not once per bit
            let mut w = word;
            while w != 0 {
                w &= w - 1; // clear the lowest set bit
                valid_count += 1;
            }
        }

        // handle remaining bits in the last partial word
        let remaining_bits = count % Self::BITS_PER_WORD;
        if remaining_bits > 0 {
            let word = self.words[full_words];
            let mask = (1u64 << remaining_bits) - 1;
            let masked_word = word & mask;

            if masked_word == mask {
                // all remaining bits are valid
                valid_count += remaining_bits;
            } else if masked_word != 0 {
                // partially valid: count set bits
                let mut w = masked_word;
                while w != 0 {
                    w &= w - 1;
                    valid_count += 1;
                }
            }
        }

        valid_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bitmap_all_valid() {
        let bitmap = Bitmap::new(100);
        assert_eq!(bitmap.len(), 100);

        for i in 0..100 {
            assert!(bitmap.is_valid(i), "Bit {} should be valid", i);
            assert!(!bitmap.is_null(i), "Bit {} should not be NULL", i);
        }

        assert!(bitmap.all_valid());
    }

    #[test]
    fn test_new_all_null() {
        let bitmap = Bitmap::new_all_null(100);

        for i in 0..100 {
            assert!(bitmap.is_null(i), "Bit {} should be NULL", i);
            assert!(!bitmap.is_valid(i), "Bit {} should not be valid", i);
        }

        assert!(!bitmap.all_valid());
    }

    #[test]
    fn test_set_operations() {
        let mut bitmap = Bitmap::new(10);

        // set some bits to NULL
        bitmap.set_null(3);
        bitmap.set_null(7);

        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_valid(1));
        assert!(bitmap.is_valid(2));
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_valid(4));
        assert!(bitmap.is_valid(5));
        assert!(bitmap.is_valid(6));
        assert!(bitmap.is_null(7));
        assert!(bitmap.is_valid(8));
        assert!(bitmap.is_valid(9));

        assert!(!bitmap.all_valid());

        // set back to valid
        bitmap.set_valid(3);
        bitmap.set_valid(7);

        assert!(bitmap.all_valid());
    }

    #[test]
    fn test_set_boolean() {
        let mut bitmap = Bitmap::new(5);

        bitmap.set(0, true); // valid
        bitmap.set(1, false); // null
        bitmap.set(2, true); // valid
        bitmap.set(3, false); // null
        bitmap.set(4, true); // valid

        assert!(bitmap.is_valid(0));
        assert!(bitmap.is_null(1));
        assert!(bitmap.is_valid(2));
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_valid(4));
    }

    #[test]
    fn test_boundary_64_bits() {
        // test at word boundary (64 bits)
        let mut bitmap = Bitmap::new(128);

        bitmap.set_null(63); // last bit of first word
        bitmap.set_null(64); // first bit of second word

        assert!(bitmap.is_null(63));
        assert!(bitmap.is_null(64));
        assert!(bitmap.is_valid(62));
        assert!(bitmap.is_valid(65));
    }

    #[test]
    fn test_clear_and_reset() {
        let mut bitmap = Bitmap::new(100);

        bitmap.set_null(10);
        bitmap.set_null(50);

        assert!(!bitmap.all_valid());

        // reset to all valid
        bitmap.reset_all_valid();
        assert!(bitmap.all_valid());

        // clear to all NULL
        bitmap.clear();
        for i in 0..100 {
            assert!(bitmap.is_null(i));
        }
    }

    #[test]
    fn test_resize() {
        let mut bitmap = Bitmap::new(10);
        bitmap.set_null(5);

        // resize larger
        bitmap.resize(20);
        assert_eq!(bitmap.len(), 20);
        assert!(bitmap.is_null(5));
        assert!(bitmap.is_valid(15)); // new bits are valid
    }
}
