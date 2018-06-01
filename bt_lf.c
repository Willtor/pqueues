#include "bt_lf.h"
#include <assert.h>
#include <forkscan.h>


typedef struct _seek_record_t {
    bt_lf_node_ptr ancestor, successor, parent, leaf;
} seek_record_t;

enum REMOVE_STATE {INJECTION, CLEANUP};

typedef struct _bt_lf_node_unpacked_t {
    bool flagged, tagged;
    bt_lf_node_ptr address;
} bt_lf_node_unpacked_t;


bt_lf_node_t* bt_lf_node_create(int64_t key){
    bt_lf_node_t* node = forkscan_malloc(sizeof(bt_lf_node_t));
    node->key = key;
    node->left = NULL;
    node->right = NULL;
    return node;
}

bt_lf_node_ptr bt_lf_node_address(bt_lf_node_ptr node){
    return (bt_lf_node_ptr)(((size_t)node) & (~0x3));
}

bt_lf_node_ptr bt_lf_node_flag(bt_lf_node_ptr node, bool flag){
    if(flag){
        return (bt_lf_node_ptr)((size_t)node | 0x1);
    } else {
        return (bt_lf_node_ptr)((size_t)node & (~0x1));
    }
}

bool bt_lf_node_is_flagged(bt_lf_node_ptr node){
    return ((size_t)node & 0x1) == 1;
}

bt_lf_node_ptr bt_lf_node_tag(bt_lf_node_ptr node, bool tag){
    if(tag){
        return (bt_lf_node_ptr)((size_t)node | 0x2);
    } else {
        return (bt_lf_node_ptr)((size_t)node & (~0x2));
    }
}

bool bt_lf_node_is_tagged(bt_lf_node_ptr node){
    return ((size_t)node & 0x2) == 2;
}

bt_lf_node_ptr bt_lf_node_pack(bt_lf_node_ptr node, bool flag, bool tag){
    return bt_lf_node_tag(bt_lf_node_flag(bt_lf_node_address(node), flag), tag);
}

bt_lf_node_unpacked_t bt_lf_node_unpack(bt_lf_node_ptr node){
    return (bt_lf_node_unpacked_t){
        .flagged = bt_lf_node_is_flagged(node),
        .tagged = bt_lf_node_is_tagged(node),
        .address = bt_lf_node_address(node)
        };
}

bt_lf_t* bt_lf_create(){
    bt_lf_t * bt_lf = forkscan_malloc(sizeof(bt_lf_t));
    bt_lf->R = bt_lf_node_create(INT64_MAX);
    bt_lf->S = bt_lf_node_create(INT64_MAX - 1);
    bt_lf->R->left = bt_lf->S;
    bt_lf->S->left = bt_lf_node_create(INT64_MAX - 2);
    bt_lf->S->right = bt_lf_node_create(INT64_MAX - 1);
    return bt_lf;
}

void bt_lf_init_seek_record(bt_lf_t *bt_lf, seek_record_t* sr){
    sr->ancestor = bt_lf->R;
    sr->successor = bt_lf->S;
    sr->parent = bt_lf->S;
    sr->leaf = bt_lf_node_address(bt_lf->S->left);
}

bt_lf_node_ptr bt_lf_node_setup(int64_t key, int64_t sibbling_key, bt_lf_node_ptr sibbling_node){
    bt_lf_node_t * node = bt_lf_node_create(key);
    bt_lf_node_t * internal_node = bt_lf_node_create(key);
    if(key < sibbling_key){
        internal_node->left = node;
        internal_node->right = sibbling_node;
        internal_node->key = sibbling_key;
    } else {
        internal_node->left = sibbling_node;
        internal_node->right = node;
    }
    return internal_node;
}

void bt_lf_seek(bt_lf_t * set, seek_record_t * sr, int64_t key){
    bt_lf_init_seek_record(set, sr);
    volatile bt_lf_node_t * parent_field = sr->parent->left;
    volatile bt_lf_node_t * current_field = sr->leaf->left;
    volatile bt_lf_node_t * current = bt_lf_node_address(current_field);

    while(current != NULL){
        if(!bt_lf_node_is_tagged(parent_field)){
            sr->ancestor = sr->parent;
            sr->successor = sr->leaf;
        }
        sr->parent = sr->leaf;
        sr->leaf = current;
        parent_field = current_field;
        if(key < current->key){
            current_field = current->left;
        }else {
            current_field = current->right;
        }
        current = bt_lf_node_address(current_field);
    }
}

bool bt_lf_cleanup(bt_lf_t * set, seek_record_t *sr, int64_t key) {
    bt_lf_node_ptr ancestor = sr->ancestor, successor = sr->successor, parent = sr->parent, leaf = sr->leaf;

    bt_lf_node_ptr volatile* successor_address = NULL;
    if(key < ancestor->key) {
        successor_address = &ancestor->left;
    } else {
        successor_address = &ancestor->right;
    }
    bt_lf_node_ptr child_val = NULL;
    bt_lf_node_ptr volatile* child_address = NULL;
    bt_lf_node_ptr sibling_val = NULL;
    bt_lf_node_ptr volatile* sibling_address = NULL;
    if(key < parent->key) {
        child_val = parent->left;
        child_address = &parent->left;
        sibling_val = parent->right;
        sibling_address = &parent->right;
    } else {
        child_val = parent->right;
        child_address = &parent->right;
        sibling_val = parent->left;
        sibling_address = &parent->left;
    }
    bt_lf_node_unpacked_t unpacked_node = bt_lf_node_unpack(*child_address);
    if(!unpacked_node.flagged) {
        sibling_val = child_val;
        sibling_address = child_address;
    }

    __sync_bool_compare_and_swap(sibling_address,
        sibling_val,
        bt_lf_node_tag(bt_lf_node_address(sibling_val), true));
    bt_lf_node_unpacked_t unpacked_sibbling = bt_lf_node_unpack(*sibling_address);
    bool result = __sync_bool_compare_and_swap(successor_address,
        bt_lf_node_address(successor),
        bt_lf_node_flag(unpacked_sibbling.address, unpacked_sibbling.flagged));
    return result;
}

int bt_lf_contains(bt_lf_t *set, int64_t key) {
    seek_record_t sr;
    bt_lf_seek(set, &sr, key);
    return sr.leaf->key == key;
}


int bt_lf_add(bt_lf_t *set, int64_t key) {
    while(true) {
        seek_record_t sr;
        bt_lf_seek(set, &sr, key);
        int64_t leaf_key = sr.leaf->key;
        if(leaf_key != key) {
            bt_lf_node_ptr parent = sr.parent;
            bt_lf_node_ptr leaf = sr.leaf;
            bt_lf_node_ptr volatile* child_address = NULL;
            int64_t parent_key = parent->key;
            if(key < parent_key) {
                child_address = &parent->left;
            } else {
                child_address = &parent->right;
            }
            bt_lf_node_ptr internal_node = bt_lf_node_setup(key, leaf_key, leaf);
            bool result = __sync_bool_compare_and_swap(child_address,
                bt_lf_node_address(leaf),
                internal_node);
            if(result) {
                return true;
            } else {
                if(key < leaf_key) {
                    forkscan_free((void *)internal_node->left);
                } else {
                    forkscan_free((void *)internal_node->right);
                }
                forkscan_free((void *)internal_node);
                bt_lf_node_unpacked_t unpacked_node = bt_lf_node_unpack(*child_address);
                if(unpacked_node.address == leaf &&
                    (unpacked_node.flagged || unpacked_node.tagged)){
                    bool done = bt_lf_cleanup(set, &sr, key);
                }
            }
        } else {
            return false;
        }
    }
}

int bt_lf_remove_leaky(bt_lf_t * set, int64_t key) {
    enum REMOVE_STATE mode = INJECTION;
    bt_lf_node_ptr leaf = NULL;
    while(true) {
        seek_record_t sr;
        bt_lf_seek(set, &sr, key);
        bt_lf_node_ptr parent = sr.parent;
        bt_lf_node_ptr volatile* child_address = NULL;
        int64_t parent_key = parent->key;
        if(key < parent_key) {
            child_address = &parent->left;
        } else {
            child_address = &parent->right;
        }
        if(mode == INJECTION) {
            leaf = sr.leaf;
            if(leaf->key != key) {
                return false;
            }
            bool result = __sync_bool_compare_and_swap(child_address,
                bt_lf_node_address(leaf),
                bt_lf_node_flag(leaf, true));
            if(result) {
                mode = CLEANUP;
                bool done = bt_lf_cleanup(set, &sr, key);
                if(done) {
                    return true;
                }
            } else {
                bt_lf_node_unpacked_t unpacked_node = bt_lf_node_unpack(*child_address);
                if(unpacked_node.address == leaf &&
                    (unpacked_node.flagged || unpacked_node.tagged)){
                    bool done = bt_lf_cleanup(set, &sr, key);
                }
            }
        } else {
            if(sr.leaf != leaf) {
                return true;
            } else {
                bool done = bt_lf_cleanup(set, &sr, key);
                if(done) {
                    return true;
                }
            }
        }
    }
}

int bt_lf_remove_retire(bt_lf_t * set, int64_t key) {
    enum REMOVE_STATE mode = INJECTION;
    bt_lf_node_ptr leaf = NULL;
    while(true) {
        seek_record_t sr;
        bt_lf_seek(set, &sr, key);
        bt_lf_node_ptr parent = sr.parent;
        bt_lf_node_ptr volatile* child_address = NULL;
        int64_t parent_key = parent->key;
        if(key < parent_key) {
            child_address = &parent->left;
        } else {
            child_address = &parent->right;
        }
        if(mode == INJECTION) {
            leaf = sr.leaf;
            if(leaf->key != key) {
                return false;
            }
            bool result = __sync_bool_compare_and_swap(child_address,
                bt_lf_node_address(leaf),
                bt_lf_node_flag(leaf, true));
            if(result) {
                mode = CLEANUP;
                bool done = bt_lf_cleanup(set, &sr, key);
                if(done) {
                    forkscan_retire((void *)leaf);
                    return true;
                }
            } else {
                bt_lf_node_unpacked_t unpacked_node = bt_lf_node_unpack(*child_address);
                if(unpacked_node.address == leaf &&
                    (unpacked_node.flagged || unpacked_node.tagged)){
                    bool done = bt_lf_cleanup(set, &sr, key);
                }
            }
        } else {
            if(sr.leaf != leaf) {
                forkscan_retire((void *)leaf);
                return true;
            } else {
                bool done = bt_lf_cleanup(set, &sr, key);
                if(done) {
                    forkscan_retire((void *)leaf);
                    return true;
                }
            }
        }
    }
}