#pragma once
#include <iostream>
#include <vector>
#include <utils.hpp>

using namespace std;

template <typename T> 
class Column {
	vector<T*> tuples;
	vector<uint64_t> tupleLength;  //@TODO maybe unsgind is enough?
	vector<uint64_t> baseOffset;
	bool fixed = false;

public:
	//Column(const Column& c) = delete;
	Column(unsigned cntTask) {
        baseOffset.push_back(0);
        for (int i=0; i<cntTask; i++) {
            tuples.emplace_back();
            baseOffset.push_back(0);
            tupleLength.emplace_back();
        }
	}
	void addTuples(unsigned pos, uint64_t* tuples, uint64_t length) {
		// if (length <= 0)
		// 	return;
        this->tuples[pos] = tuples;
		this->tupleLength[pos] = length;
        //this->baseOffset[pos+1] = length + baseOffset[pos];
        // this->tuples.push_back(tuples);
		// this->tupleLength.push_back(length);
		// length += baseOffset[baseOffset.size()-1];
		// this->baseOffset.push_back(length);
	}

    void fix() {
        for (unsigned i=1; i<baseOffset.size(); i++) {
            baseOffset[i] = baseOffset[i-1]+tupleLength[i-1];
        }
        fixed = true;
    }

    class Iterator;

	Iterator begin(uint64_t index) { 
        assert (fixed);
        return Iterator(*this, index);
     }
	
	class Iterator {
		unsigned seg_index;
		uint64_t seg_offset; 
		Column<T> col;

	public:
		Iterator(Column<T>& col, uint64_t start) : col(col) {
            if (col.tuples.size() == 0)
                return;
			auto it = lower_bound(col.baseOffset.begin(), col.baseOffset.end(),  start);
			seg_index = it - col.baseOffset.begin();
            assert(seg_index < col.baseOffset.size());
			if (col.baseOffset[seg_index] != start)
			seg_index--;
				seg_offset = start - col.baseOffset[seg_index];
			while (0 == col.tupleLength[seg_index]) {
                seg_index++;
                seg_offset = 0;
                if (seg_index == col.tupleLength.size()) {
                    assert("Column::Iterator: invalud start value");
                    break;
                }
			}
		}
		
		inline T& operator*() {
			if(seg_index >= col.tupleLength.size()) {
				cout << "read col out of bound" << endl;
			}
            //assert(seg_index < col.tupleLength.size());
			return col.tuples[seg_index][seg_offset];
		}

		inline Iterator& operator++() {
			seg_offset++;
			// 读取列表中的最后一个offset的时候居然会走到下一个seg，直接导致读取越界了，原版居然也是这么写的。
			// 需要研究一下column的过程。
			while (UNLIKELY(seg_offset >= col.tupleLength[seg_index])) {
                seg_index++;
                seg_offset = 0; 
                if (UNLIKELY(seg_index == col.tupleLength.size())) {
                    break;
                }
			}
			return *this;
		}
				
	};
	//Iterator end() { return Iterator(); }
	
};
