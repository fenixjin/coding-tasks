#include "joiner.h"

#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <set>
#include <sstream>
#include <vector>

#include "parser.h"
#include "operators.h"

using namespace std;

namespace {

enum QueryGraphProvides { Left, Right, Both, None };

// Analyzes inputs of join
QueryGraphProvides analyzeInputOfJoin(std::set<unsigned> &usedRelations,
                                      SelectInfo &leftInfo,
                                      SelectInfo &rightInfo) {
    bool used_left = usedRelations.count(leftInfo.binding);
    bool used_right = usedRelations.count(rightInfo.binding);

    if (used_left ^ used_right)
        return used_left ? QueryGraphProvides::Left : QueryGraphProvides::Right;
    if (used_left && used_right)
        return QueryGraphProvides::Both;
    return QueryGraphProvides::None;
}

}

// Loads a relation_ from disk
void Joiner::addRelation(const char *file_name) {
    relations_.emplace_back(file_name);
}

void Joiner::addRelation(Relation &&relation) {
    relations_.emplace_back(std::move(relation));
}

// Loads a relation from disk
const Relation &Joiner::getRelation(unsigned relation_id) {
    if (relation_id >= relations_.size()) {
        std::cerr << "Relation with id: " << relation_id << " does not exist"
                  << std::endl;
        throw;
    }
    return relations_[relation_id];
}

// Add scan to query
std::unique_ptr<Operator> Joiner::addScan(std::set<unsigned> &used_relations,
        const SelectInfo &info,
        QueryInfo &query) {
    used_relations.emplace(info.binding);
    std::vector<FilterInfo> filters;
    for (auto &f : query.filters()) {
        if (f.filter_column.binding == info.binding) {
            filters.emplace_back(f);
        }
    }
    return !filters.empty() ?
           std::make_shared<FilterScan>(getRelation(info.rel_id), filters)
           : std::make_shared<Scan>(getRelation(info.rel_id),
                                    info.binding);
}

// Executes a join query
void Joiner::join(QueryInfo &query, int query_index) {
    if(query.illegalQuery()) {
        return ;
    }
    std::set<unsigned> used_relations;

    // We always start with the first join predicate and append the other joins
    // to it (--> left-deep join trees). You might want to choose a smarter
    // join ordering ...
    const auto &firstJoin = query.predicates()[0];
    std::shared_ptr<Operator> left, right;
    left = addScan(used_relations, firstJoin.left, query);
    right = addScan(used_relations, firstJoin.right, query);
    std::shared_ptr<Operator>
    root = std::make_shared<Join>(left, right, firstJoin);

    left->setParent(root);
    right->setParent(root);

    auto& predicates_copy = query.predicates();

    // remove duplicate predicates 
    std::sort(predicates_copy.begin(), predicates_copy.end());
    auto last = std::unique(predicates_copy.begin(), predicates_copy.end());
    predicates_copy.erase(last, predicates_copy.end());

    for (unsigned i = 1; i < predicates_copy.size(); ++i) {
        auto &p_info = predicates_copy[i];
        auto &left_info = p_info.left;
        auto &right_info = p_info.right;
        
        switch (analyzeInputOfJoin(used_relations, left_info, right_info)) {
        case QueryGraphProvides::Left:
            left = root;
            right = addScan(used_relations, right_info, query);
            root = std::make_shared<Join>(left, right, p_info);
            left->setParent(root);
            right->setParent(root);
            break;
        case QueryGraphProvides::Right:
            left = addScan(used_relations,
                           left_info,
                           query);
            right = root;
            root = std::make_shared<Join>(left, right, p_info);
            break;
        case QueryGraphProvides::Both:
            // All relations of this join are already used somewhere else in the
            // query. Thus, we have either a cycle in our join graph or more than
            // one join predicate per join.
            root = std::make_shared<SelfJoin>(root, p_info);
            break;
        case QueryGraphProvides::None:
            // Process this predicate later when we can connect it to the other
            // joins. We never have cross products.
            predicates_copy.push_back(p_info);
            break;
        };
    }

    std::shared_ptr<Checksum> checksum = std::make_shared<Checksum>(*this, root, query.selections());
    root->setParent(checksum);
    asyncJoins[query_index] = checksum;
    checksum->asyncRun(ioService,query_index);

    return;
}

void Joiner::waitAsyncJoins() {
    unique_lock<mutex> lk(cvAsyncMutex);
    if (pendingAsyncJoin > 0)  {
#ifdef VERBOSE
        cout << "Joiner::waitAsyncJoins wait" << endl;
#endif
        cvAsync.wait(lk); 
#ifdef VERBOSE
        cout << "Joiner::waitAsyncJoins wakeup" << endl;
#endif
    }
}

void Joiner::createAsyncQueryTask(string line)
{
	__sync_fetch_and_add(&pendingAsyncJoin, 1);
    QueryInfo query;
    query.parseQuery(line);
    asyncJoins.emplace_back();
    asyncResults.emplace_back();
    
    ioService.post(bind(&Joiner::join, this, query, nextQueryIndex)); 
    __sync_fetch_and_add(&nextQueryIndex, 1);
}

