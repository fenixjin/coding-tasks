#include "parser.hpp"

#include <cassert>
#include <iostream>
#include <utility>
#include <sstream>

namespace {

// Split a line into numbers
static void splitString(std::string &line,
                        std::vector<unsigned> &result,
                        const char delimiter) {
    std::stringstream ss(line);
    std::string token;
    while (getline(ss, token, delimiter)) {
        result.push_back(stoul(token));
    }
}

// Parse a line into std::strings
static void splitString(std::string &line,
                        std::vector<std::string> &result,
                        const char delimiter) {
    std::stringstream ss(line);
    std::string token;
    while (getline(ss, token, delimiter)) {
        result.push_back(token);
    }
}

// Split a line into predicate std::strings
static void splitPredicates(std::string &line,
                            std::vector<std::string> &result) {
    // Determine predicate type
    for (auto ct : comparisonTypes) {
        if (line.find(ct) != std::string::npos) {
            splitString(line, result, ct);
            break;
        }
    }
}

// Resolve relation_ id
static void resolveIds(std::vector<unsigned> &relation_ids,
                       SelectInfo &select_info) {
    select_info.rel_id = relation_ids[select_info.binding];
}

inline static bool isConstant(std::string &raw) {
    return raw.find('.') == std::string::npos;
}

// Wraps relation_ id into quotes to be a SQL compliant std::string
static std::string wrapRelationName(uint64_t id) {
    return "\"" + std::to_string(id) + "\"";
}

}

// Parse a std::string of relation ids
void QueryInfo::parseRelationIds(std::string &raw_relations) {
    splitString(raw_relations, relation_ids_, ' ');
}

static SelectInfo parseRelColPair(std::string &raw) {
    std::vector<unsigned> ids;
    splitString(raw, ids, '.');
    return SelectInfo(0, ids[0], ids[1]);
}

// Parse a single predicate:
// join "r1Id.col1Id=r2Id.col2Id" or "r1Id.col1Id=constant" filter
void QueryInfo::parsePredicate(std::string &raw_predicate) {
    std::vector<std::string> rel_cols;
    splitPredicates(raw_predicate, rel_cols);
    assert(rel_cols.size() == 2);
    assert(!isConstant(rel_cols[0])
           && "left_ side of a predicate is always a SelectInfo");
    auto left_select = parseRelColPair(rel_cols[0]);
    if (isConstant(rel_cols[1])) {
        uint64_t constant = stoul(rel_cols[1]);
        char comp_type = raw_predicate[rel_cols[0].size()];
        filters_.emplace_back(left_select,
                              constant,
                              FilterInfo::Comparison(comp_type));
    } else {
        auto right_select = parseRelColPair(rel_cols[1]);
        if(right_select < left_select) {
            std::swap(left_select, right_select);
        }
        predicates_.emplace_back(left_select, right_select);
    }
}

// Parse predicates
void QueryInfo::parsePredicates(std::string &raw_predicates) {
    std::vector<std::string> predicate_strings;
    splitString(raw_predicates, predicate_strings, '&');
    for (auto &raw_predicate : predicate_strings) {
        parsePredicate(raw_predicate);
    }
    // propagate filter to make temp table smaller
    for(int i = 0; i < filters_.size(); i++) { 
        for(auto& predicate : predicates_) {
            if(filters_[i].filter_column == predicate.left) {
                FilterInfo new_filter{predicate.right, filters_[i].constant, filters_[i].comparison};
                if (std::find(filters_.begin(), filters_.end(), new_filter) == filters_.end()) {
                    filters_.push_back(std::move(new_filter));
                }
            }
            if(filters_[i].filter_column == predicate.right) {
                FilterInfo new_filter{predicate.left, filters_[i].constant, filters_[i].comparison};
                if (std::find(filters_.begin(), filters_.end(), new_filter) == filters_.end()) {
                    filters_.push_back(std::move(new_filter));
                }
            }
        }
    }
    // combine overlapped predicates and check for contradictory predicates;
    std::vector<CombinedFilterInfo> col_2_combine_filters;
    for(auto& f: filters_) {
        bool found = false;
        for(auto& combined_filter : col_2_combine_filters) {
            if(f.filter_column == combined_filter.filter_column) {
                found = true;
                if(combined_filter.mergeCondition(f.comparison, f.constant)) {
                    setIllegalQuery();
                    return;
                }
                break;
            }
        }
        if (!found) {
            col_2_combine_filters.emplace_back(f.filter_column, f.comparison, f.constant);
        }
    }
    filters_.clear();

    // replace filter with optimized value
    for(auto& combined_filter : col_2_combine_filters) {
        if(combined_filter.equal.has_value()){
            filters_.emplace_back(
                combined_filter.filter_column, 
                combined_filter.equal.value(), 
                FilterInfo::Comparison::Equal
            );
        } else {
            if(combined_filter.greater.has_value()) {
                filters_.emplace_back(
                    combined_filter.filter_column, 
                    combined_filter.greater.value(), 
                    FilterInfo::Comparison::Greater
                );
            }
            if(combined_filter.less.has_value()) {
                filters_.emplace_back(
                    combined_filter.filter_column, 
                    combined_filter.less.value(), 
                    FilterInfo::Comparison::Less
                );
            }
        }
    }
}

// Parse selections
void QueryInfo::parseSelections(std::string &raw_selections) {
    std::vector<std::string> selection_strings;
    splitString(raw_selections, selection_strings, ' ');
    for (auto &raw_select : selection_strings) {
        selections_.emplace_back(parseRelColPair(raw_select));
    }
}

// Resolve relation ids
void QueryInfo::resolveRelationIds() {
    // Selections
    for (auto &s_info : selections_) {
        resolveIds(relation_ids_, s_info);
    }
    // Predicates
    for (auto &p_info : predicates_) {
        resolveIds(relation_ids_, p_info.left);
        resolveIds(relation_ids_, p_info.right);
    }
    // Filters
    for (auto &f_info : filters_) {
        resolveIds(relation_ids_, f_info.filter_column);
    }
}

// Parse query [RELATIONS]|[PREDICATES]|[SELECTS]
void QueryInfo::parseQuery(std::string &raw_query) {
    clear();
    std::vector<std::string> query_parts;
    splitString(raw_query, query_parts, '|');
    assert(query_parts.size() == 3);
    parseRelationIds(query_parts[0]);
    parsePredicates(query_parts[1]);
    reorderPredicates();
    parseSelections(query_parts[2]);
    resolveRelationIds();
}

void QueryInfo::reorderPredicates() {
    sort(predicates_.begin(), predicates_.end(), [&](const PredicateInfo& a,
    const PredicateInfo& b) -> bool {
        int a_score = 0, b_score = 0;
        for(auto& f : filters_) {
            if(f.filter_column.binding == a.left.binding 
                || f.filter_column.binding == a.right.binding) {
                    if (f.comparison == FilterInfo::Comparison::Equal) {
                        a_score += 1000;
                    } else {
                        a_score += 1;
                    }
            }
            if (f.filter_column.binding == b.left.binding
                || f.filter_column.binding == b.right.binding) {
                    if (f.comparison == FilterInfo::Comparison::Equal) {
                        b_score += 1000;
                    } else {
                        b_score += 1;
                    }
            }
            return a_score > b_score;
        }
        return false;
    });
}
// Reset query info
void QueryInfo::clear() {
    relation_ids_.clear();
    predicates_.clear();
    filters_.clear();
    selections_.clear();
}

// Appends a selection info to the stream
std::string SelectInfo::dumpSQL(bool add_sum) {
    auto inner_part = wrapRelationName(binding) + ".c" + std::to_string(col_id);
    return add_sum ? "SUM(" + inner_part + ")" : inner_part;
}

// Dump text format
std::string SelectInfo::dumpText() {
    return std::to_string(binding) + "." + std::to_string(col_id);
}

// Dump text format
std::string FilterInfo::dumpText() {
    return filter_column.dumpText() + static_cast<char>(comparison)
           + std::to_string(constant);
}

// Dump text format
std::string FilterInfo::dumpSQL() {
    return filter_column.dumpSQL() + static_cast<char>(comparison)
           + std::to_string(constant);
}
// this function may not
bool CombinedFilterInfo::mergeCondition(FilterInfo::Comparison comparison, double value) {
    if(comparison == FilterInfo::Comparison::Equal) {
        if(equal.has_value() && value != equal.value()){
            return false;
        } else if(greater.has_value() && value <= greater.value()) {
            return false;
        } else if(less.has_value() && value <= less.value()) {
            return false;
        }
        equal = value;
    } else if (comparison == FilterInfo::Comparison::Greater) {
        if(equal.has_value()){
            if(value >= equal.value()) {
                return false;
            }
            return true;
        }
        if(!greater.has_value() || value > greater.value()) {
            greater.value() = value;
        }
        if(less.has_value() && value >= less.value()){
            return false;
        }
    } else if (comparison == FilterInfo::Comparison::Less) {
        if(equal.has_value() ) {
            if(value <= equal.value()){
                return false;
            }
            return true;
        }
        if(greater.has_value() && value <= greater.value()) {
            return false;
        }
        if(!less.has_value() || value < less.value()) {
            less.value() = value;
        }
    }
}

// Dump text format
std::string PredicateInfo::dumpText() {
    return left.dumpText() + '=' + right.dumpText();
}

// Dump text format
std::string PredicateInfo::dumpSQL() {
    return left.dumpSQL() + '=' + right.dumpSQL();
}

template<typename T>
static void dumpPart(std::stringstream &ss, std::vector<T> elements) {
    for (unsigned i = 0; i < elements.size(); ++i) {
        ss << elements[i].dumpText();
        if (i < elements.size() - 1)
            ss << T::delimiter;
    }
}

template<typename T>
static void dumpPartSQL(std::stringstream &ss, std::vector<T> elements) {
    for (unsigned i = 0; i < elements.size(); ++i) {
        ss << elements[i].dumpSQL();
        if (i < elements.size() - 1)
            ss << T::delimiterSQL;
    }
}

// Dump text format
std::string QueryInfo::dumpText() {
    std::stringstream text;
    // Relations
    for (unsigned i = 0; i < relation_ids_.size(); ++i) {
        text << relation_ids_[i];
        if (i < relation_ids_.size() - 1)
            text << " ";
    }
    text << "|";

    dumpPart(text, predicates_);
    if (predicates_.size() && filters_.size())
        text << PredicateInfo::delimiter;
    dumpPart(text, filters_);
    text << "|";
    dumpPart(text, selections_);

    return text.str();
}

// Dump SQL
std::string QueryInfo::dumpSQL() {
    std::stringstream sql;
    sql << "SELECT ";
    for (unsigned i = 0; i < selections_.size(); ++i) {
        sql << selections_[i].dumpSQL(true);
        if (i < selections_.size() - 1)
            sql << ", ";
    }

    sql << " FROM ";
    for (unsigned i = 0; i < relation_ids_.size(); ++i) {
        sql << "r" << relation_ids_[i] << " " << wrapRelationName(i);
        if (i < relation_ids_.size() - 1)
            sql << ", ";
    }

    sql << " WHERE ";
    dumpPartSQL(sql, predicates_);
    if (!predicates_.empty() && !filters_.empty())
        sql << " and ";
    dumpPartSQL(sql, filters_);

    sql << ";";

    return sql.str();
}

QueryInfo::QueryInfo(std::string raw_query) {
    parseQuery(raw_query);
}

