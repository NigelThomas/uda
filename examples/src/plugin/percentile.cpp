/*
// Aspen dataflow server
// Copyright (C) 2020-2020 SQLstream, Inc.
// Copyright (C) 2020 Guavus, Inc.
*/
#include "sqlstream/Udf.h"

using namespace sqlstream;
using fennel::SqlState;

// a percentile UDA

template<typename TYPE>
class Percentile : public ComplexAnalytic
{
    std::multiset<TYPE> hist;
public:
    void init() {
        hist.clear();
    }

    inline void add(CalculatorContext ctx, TYPE value) {
        //ctx.logInfo("add");
        //fprintf(stderr, "percentile add %f\n", value);
        hist.insert(value);
    }

    inline void drop(CalculatorContext ctx, TYPE value) {
        //ctx.logInfo("drop");
        //fprintf(stderr, "percentile drop %f\n", value);
        auto search = hist.find(value);
        if (search != hist.end()) {
            search = hist.erase(search);
        }
    }

    inline void dropAccumulator(CalculatorContext ctx, varbinary_t other) {
        //ctx.logInfo("dropAccumulator");
        double *bufptr = reinterpret_cast<double *>(other.data);
        for (int i = 0; i < other.size; i += sizeof(double)) {
            drop(ctx, *bufptr++);
        }
    }

    inline void addAccumulator(CalculatorContext ctx, varbinary_t other) {
        //ctx.logInfo("addAccumulator");
        double *bufptr = reinterpret_cast<double *>(other.data);
        for (int i = 0; i < other.size; i += sizeof(double)) {
            add(ctx, *bufptr++);
        }
    }

    int serialize(uint8_t *buf, uint &len, uint maxSerializedSize, int chunkIndex) {
        // serialize multiset as an array
        // how many doubles can we fit in? 
        size_t maxItemCount = maxSerializedSize / sizeof(double); // - 1;

        // which elements will go into current chunk?
        size_t setElems = hist.size();
        size_t startpos = chunkIndex * maxItemCount;
        size_t endpos = std::min(startpos + maxItemCount, setElems);        
        uint32_t chunkElems = endpos - startpos;

        // copy them in
        double *dbufptr = reinterpret_cast<double *>(buf);       
        auto ptr = hist.cbegin();
    
        for (std::advance(ptr,startpos); startpos < endpos && ptr != hist.end(); std::next(ptr), startpos++) {
            *dbufptr++ = *ptr;
        }

        // return chunk length
        len = (uint) chunkElems * sizeof(double);

        // do we need another chunk?
        return (endpos < hist.size())? chunkIndex + 1 : -1;
    }

  
    // PERCENTILE_CONT:
    // 0 <= percentile <= 1
    inline void percentile_cont(CalculatorContext ctx, const ResultRegister<TYPE> &result, double position) const{

        if (position < 0 || position > 1) {
            ctx.logSevere("position out of range 0 <= position <= 1");
            ctx.throwException(fennel::SqlState::instance().code22023());
        }

        // calculate rownumber based at 0        
        double rn = position * (hist.size() - 1);
        size_t frn = floor(rn);
        size_t crn = ceil(rn);

        auto ptr = hist.cbegin();
        std::advance(ptr, frn);

        double frnval = *ptr;

        if (crn == frn) {
            result = frnval;
        } else {
            // advance what should be one place
            std::advance(ptr, crn - frn);
            double crnval = *ptr;
            // weighted interpolation
            result = (crn - rn) * frnval + (rn - frn) * crnval;
        }
    }

    // PERCENTILE_DISC: 
    // returns the value from the group set which has the smallest cumulative distribution value (corresponding to the given sort order), which is larger than or equal to the specified percentile value.

    inline void percentile_disc(CalculatorContext ctx, const ResultRegister<TYPE> &result, double position) const{

        if (position < 0 || position > 1) {
            ctx.logSevere("position out of range 0 <= position <= 1");
            ctx.throwException(fennel::SqlState::instance().code22023());
        }
        

        size_t histPos = std::min(hist.size(),(size_t)(ceil(hist.size() * position))) - (size_t) 1;
        if (histPos < 0) histPos = 0;

        auto ptr = hist.cbegin();
        std::advance(ptr, histPos);
        //fprintf(stderr, "percentile_dist %f, %d, %f\n", position, (int) histPos, *ptr);
        result = *ptr;
    }

};
INSTALL_BASE_ANALYTIC(percentileDouble, Percentile<double>);
INSTALL_UDA_RESULT_FUNCTION(percentile_contDouble, percentileDouble, Percentile<double>, percentile_cont);
INSTALL_UDA_RESULT_FUNCTION(percentile_discDouble, percentileDouble, Percentile<double>, percentile_disc);

// End percentile.cpp
