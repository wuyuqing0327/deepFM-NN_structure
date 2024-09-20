import warnings
warnings.filterwarnings("ignore")
import numpy as np 
import pandas as pd
from tools import auroc, cal_ks, auc, ks, fea_psi_calc
from sklearn import metrics
import sys
import math
# from sklearn.metrics import roc_auc_score
# import gc
import random
# from sklearn.metrics import r2_score
import tensorflow as tf
import numpy as np
from rnn import dynamic_rnn
from tensorflow.python.ops.rnn_cell import *
# from tensorflow.contrib.rnn.python.ops.core_rnn_cell import _linear as _Linear
# from tensorflow.contrib.rnn.python.ops.core_rnn_cell import _Linear

from tensorflow import keras
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import init_ops
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import variable_scope as vs
from keras import backend as K
from tensorflow.python.ops.rnn_cell import RNNCell, GRUCell

from tensorflow.python.framework import constant_op
from tensorflow.python.framework import dtypes
from tensorflow.python.framework import ops
from tensorflow.python.ops import array_ops
from tensorflow.python.ops import embedding_ops
from tensorflow.python.ops import init_ops
from tensorflow.python.ops import math_ops
from tensorflow.python.ops import nn_ops
from tensorflow.python.ops import rnn_cell_impl
from tensorflow.python.ops import variable_scope as vs
from tensorflow.python.platform import tf_logging as logging
from tensorflow.python.util import nest


LOAN_SEQ_LENGTH=10
LOAN_FEATURE_NUM=26

REPAY_SEQ_LENGTH=15
REPAY_FEATURE_NUM=17

# EVENT_ACTION_SEQ_LENGTH=30
# EVENT_ACTION_FEATURE_NUM=2

EVENT_SEQ_LENGTH=20
EVENT_FEATURE_NUM=45

CHAIN_SEQ_LENGTH=20
CHAIN_FEATURE_NUM=10

STAT_FEATURE_NUM = 197

BATCH_SIZE=1024
ETA=0.0001
EPOCHES=100

RNNCell = rnn_cell_impl.RNNCell
_WEIGHTS_VARIABLE_NAME = rnn_cell_impl._WEIGHTS_VARIABLE_NAME
_BIAS_VARIABLE_NAME = rnn_cell_impl._BIAS_VARIABLE_NAME



class _Linear(object):
    """Linear map: sum_i(args[i] * W[i]), where W[i] is a variable.

    Args:
    args: a 2D Tensor or a list of 2D, batch, n, Tensors.
    output_size: int, second dimension of weight variable.
    dtype: data type for variables.
    build_bias: boolean, whether to build a bias variable.
    bias_initializer: starting value to initialize the bias
      (default is all zeros).
    kernel_initializer: starting value to initialize the weight.

    Raises:
    ValueError: if inputs_shape is wrong.
    """

    def __init__(self,
               args,
               output_size,
               build_bias,
               bias_initializer=None,
               kernel_initializer=None):
        self._build_bias = build_bias

        if args is None or (nest.is_sequence(args) and not args):
            raise ValueError("`args` must be specified")
        if not nest.is_sequence(args):
            args = [args]
            self._is_sequence = False
        else:
            self._is_sequence = True

        # Calculate the total size of arguments on dimension 1.
        total_arg_size = 0
        shapes = [a.get_shape() for a in args]
        for shape in shapes:
            if shape.ndims != 2:
                raise ValueError("linear is expecting 2D arguments: %s" % shapes)
            if shape.dims[1].value is None:
                raise ValueError("linear expects shape[1] to be provided for shape %s, "
                             "but saw %s" % (shape, shape[1]))
            else:
                total_arg_size += shape.dims[1].value

        dtype = [a.dtype for a in args][0]

        scope = vs.get_variable_scope()
        with vs.variable_scope(scope) as outer_scope:
            self._weights = vs.get_variable(
              _WEIGHTS_VARIABLE_NAME, [total_arg_size, output_size],
              dtype=dtype,
              initializer=kernel_initializer)
            if build_bias:
                with vs.variable_scope(outer_scope) as inner_scope:
                    inner_scope.set_partitioner(None)
                if bias_initializer is None:
                    bias_initializer = init_ops.constant_initializer(0.0, dtype=dtype)
                self._biases = vs.get_variable(
                  _BIAS_VARIABLE_NAME, [output_size],
                  dtype=dtype,
                  initializer=bias_initializer)

    def __call__(self, args):
        if not self._is_sequence:
            args = [args]

        if len(args) == 1:
            res = math_ops.matmul(args[0], self._weights)
        else:
          # Explicitly creating a one for a minor performance improvement.
            one = constant_op.constant(1, dtype=dtypes.int32)
            res = math_ops.matmul(array_ops.concat(args, one), self._weights)
        if self._build_bias:
            res = nn_ops.bias_add(res, self._biases)
        return res
    

    
class VecAttGRUCell(RNNCell):
    """Gated Recurrent Unit cell (cf. http://arxiv.org/abs/1406.1078).
    Args:
      num_units: int, The number of units in the GRU cell.
      activation: Nonlinearity to use.  Default: `tanh`.
      reuse: (optional) Python boolean describing whether to reuse variables
       in an existing scope.  If not `True`, and the existing scope already has
       the given variables, an error is raised.
      kernel_initializer: (optional) The initializer to use for the weight and
      projection matrices.
      bias_initializer: (optional) The initializer to use for the bias.
    """

    def __init__(self,
                 num_units,
                 activation=None,
                 reuse=None,
                 kernel_initializer=None,
                 bias_initializer=None):
        super(VecAttGRUCell, self).__init__(_reuse=reuse)
        self._num_units = num_units
        self._activation = activation or math_ops.tanh
        self._kernel_initializer = kernel_initializer
        self._bias_initializer = bias_initializer
        self._gate_linear = None
        self._candidate_linear = None

    @property
    def state_size(self):
        return self._num_units

    @property
    def output_size(self):
        return self._num_units

    def __call__(self, inputs, state, att_score):
        return self.call(inputs, state, att_score)

    def call(self, inputs, state, att_score=None):
        """Gated recurrent unit (GRU) with nunits cells."""
        if self._gate_linear is None:
            bias_ones = self._bias_initializer
            if self._bias_initializer is None:
                bias_ones = init_ops.constant_initializer(
                    1.0, dtype=inputs.dtype)
            with vs.variable_scope("gates"):  # Reset gate and update gate.
                self._gate_linear = _Linear(
                    [inputs, state],
                    2 * self._num_units,
                    True,
                    bias_initializer=bias_ones,
                    kernel_initializer=self._kernel_initializer)

        value = math_ops.sigmoid(self._gate_linear([inputs, state]))
        #value = math_ops.sigmoid(self._gate_linear)
        r, u = array_ops.split(value=value, num_or_size_splits=2, axis=1)

        r_state = r * state
        if self._candidate_linear is None:
            with vs.variable_scope("candidate"):
                self._candidate_linear = _Linear(
                    [inputs, r_state],
                    self._num_units,
                    True,
                    bias_initializer=self._bias_initializer,
                    kernel_initializer=self._kernel_initializer)
#         print(self._candidate_linear)
        c = self._activation(self._candidate_linear([inputs, r_state]))
#         c = self._activation(self._candidate_linear)
        u = (1.0 - att_score) * u
        new_h = u * state + (1 - u) * c
        return new_h, new_h

    
def din_fcn_attention(query, facts, attention_size, mask, stag='null', mode='SUM', softmax_stag=1, time_major=False, return_alphas=False, forCnn=False):
    if isinstance(facts, tuple):
        # In case of Bi-RNN, concatenate the forward and the backward RNN outputs.
        facts = tf.concat(facts, 2)
    if len(facts.get_shape().as_list()) == 2:
        facts = tf.expand_dims(facts, 1)

    if time_major:
        # (T,B,D) => (B,T,D)
        facts = tf.array_ops.transpose(facts, [1, 0, 2])
    # Trainable parameters
    mask = tf.equal(mask, tf.ones_like(mask))
    facts_size = facts.get_shape().as_list()[-1]  # D value - hidden size of the RNN layer
    querry_size = query.get_shape().as_list()[-1]
    query = tf.layers.dense(query, facts_size, activation=None, name='f1' + stag)
    query = prelu(query)
    queries = tf.tile(query, [1, tf.shape(facts)[1]])
    queries = tf.reshape(queries, tf.shape(facts))
    din_all = tf.concat([queries, facts, queries-facts, queries*facts], axis=-1)
    d_layer_1_all = tf.layers.dense(din_all, 80, activation=tf.nn.sigmoid, name='f1_att' + stag)
    d_layer_2_all = tf.layers.dense(d_layer_1_all, 40, activation=tf.nn.sigmoid, name='f2_att' + stag)
    d_layer_3_all = tf.layers.dense(d_layer_2_all, 1, activation=None, name='f3_att' + stag)
    d_layer_3_all = tf.reshape(d_layer_3_all, [-1, 1, tf.shape(facts)[1]])
    scores = d_layer_3_all
    # Mask
    # key_masks = tf.sequence_mask(facts_length, tf.shape(facts)[1])   # [B, T]
    key_masks = tf.expand_dims(mask, 1) # [B, 1, T]
    paddings = tf.ones_like(scores) * (-2 ** 32 + 1)
    if not forCnn:
        scores = tf.where(key_masks, scores, paddings)  # [B, 1, T]

    # Scale
    # scores = scores / (facts.get_shape().as_list()[-1] ** 0.5)

    # Activation
    if softmax_stag:
        scores = tf.nn.softmax(scores)  # [B, 1, T]

    # Weighted sum
    if mode == 'SUM':
        output = tf.matmul(scores, facts)  # [B, 1, H]
        # output = tf.reshape(output, [-1, tf.shape(facts)[-1]])
    else:
        scores = tf.reshape(scores, [-1, tf.shape(facts)[1]])
        output = facts * tf.expand_dims(scores, -1) # tf.expand_dims(scores, -1) => [B, T, 1], facts => [B,T,H]
        output = tf.reshape(output, tf.shape(facts)) # [B, T, H]
    if return_alphas:
        return output, scores
    return output


def prelu(_x, scope=''):
    """parametric ReLU activation"""
    with tf.variable_scope(name_or_scope=scope, default_name="prelu"):
        _alpha = tf.get_variable("prelu_"+scope, shape=_x.get_shape()[-1],
                                 dtype=_x.dtype, initializer=tf.constant_initializer(0.1))
        return tf.maximum(0.0, _x) + _alpha * tf.minimum(0.0, _x)
    

def auxiliary_net(input_x, stag='auxiliary_net', reuse=tf.AUTO_REUSE):
    with tf.variable_scope("aux", reuse=reuse):
        bn1 = tf.layers.batch_normalization(inputs=input_x, name='bn1' + stag, reuse=tf.AUTO_REUSE, training=True)
        dnn1 = tf.layers.dense(bn1, 32, activation=None, name='f1' + stag, reuse=tf.AUTO_REUSE)
        dnn1 = tf.nn.relu(dnn1)
        dnn2 = tf.layers.dense(dnn1, 1, activation=None, name='f2' + stag, reuse=tf.AUTO_REUSE)
        y_hat = tf.nn.sigmoid(dnn2)
    return y_hat


def auxiliary_loss(h_states, click_label, mask, stag=None):
    #h_states = tf.placeholder(tf.float32, [None, 10, 32])
    #mask = tf.placeholder(tf.float32, [None, 10, 1])
    # click_label = tf.placeholder(tf.float32, [None, 10, 1])
    # 每一个 用户状态序列中，如果长度不足，则将 click_prob 中根据补零数据预测出来的部分转化成0，同时label中不足长度的部分标记为0，以此来做处理
    click_prob = auxiliary_net(h_states) * mask
    loss=tf.reduce_mean(-tf.reduce_mean(1.0*click_label*tf.log(click_prob) + (1-click_label)*tf.log(1-click_prob), reduction_indices=1))
    return loss


def single_layer_dynamic_gru(input_x, n_steps, n_hidden, name):
        '''
        返回动态单层GRU单元的输出，以及cell状态
        args:
            input_x:输入张量 形状为[batch_size,n_steps,n_input]
            n_steps:时序总数
            n_hidden：gru单元输出的节点个数 即隐藏层节点数
        '''
        with tf.variable_scope(name):
            #可以看做隐藏层
            gru_cell = tf.nn.rnn_cell.GRUCell(num_units=n_hidden)
            lstm_cell = tf.nn.rnn_cell.BasicLSTMCell(n_hidden,forget_bias=1.0,state_is_tuple=True)
            #动态rnn函数传入的是一个三维张量，[batch_size,n_steps,n_input]  输出也是这种形状
            hiddens,states = tf.nn.dynamic_rnn(cell=lstm_cell,inputs=input_x,dtype=tf.float32)
            #注意这里输出需要转置  转换为时序优先的
            hiddens = tf.transpose(hiddens,[1,0,2]) # 转换之后形状是[n_step, batch_size, n_hidden]
        return hiddens,states


    
def get_batch(batch_size,
                  stat_fea,
                  train_loan_seq_matrix,
                  train_repay_seq_matrix,
                  train_chain_seq_matrix,
                  train_account_seq_matrix,
                  hist_account_seq_length_train_data,
                  target_account_train_data,
                  mask_train_data,
                  train_account_seq_aux_label,
                  train_label, 
                  data_num
                 ):
    length = data_num
    
    for i in range(int(math.floor(length/batch_size))):
        a = stat_fea[i*batch_size:(i+1)*batch_size, :]
        
        b = train_loan_seq_matrix[i*batch_size:(i+1)*batch_size, :, :]
        
        c = train_repay_seq_matrix[i*batch_size:(i+1)*batch_size, :, :]
        
        d = train_chain_seq_matrix[i*batch_size:(i+1)*batch_size, :, :]
        
        e = train_account_seq_matrix[i*batch_size:(i+1)*batch_size, :, :]
            
        f = hist_account_seq_length_train_data[i*batch_size:(i+1)*batch_size]
        
        g = target_account_train_data[i*batch_size:(i+1)*batch_size, :]
            
        h = mask_train_data[i*batch_size:(i+1)*batch_size, :]
        
        k = train_account_seq_aux_label[i*batch_size:(i+1)*batch_size, :, :]
            
        j = train_label[i*batch_size:(i+1)*batch_size]
        
        yield [a,b,c,d,e,f,g,h,k,j] 
        

        

        
        
# ---------------------- define dien model tensor graph ------------------------------
tf.reset_default_graph()

# --------------------- define placeholders for graph ---------------------
hist_account_seq = tf.placeholder(tf.float32, [None, EVENT_SEQ_LENGTH, EVENT_FEATURE_NUM]) #[B, T, FEA_NUM]

hist_account_seq_length = tf.placeholder(tf.int32, [None]) #[B]

target_account = tf.placeholder(tf.float32,[None, EVENT_FEATURE_NUM]) #[B, FEA_NUM]

mask = tf.placeholder(tf.float32, [None, None]) # [B, T]

hist_click_label = tf.placeholder(tf.float32, [None, EVENT_SEQ_LENGTH, 1]) #[B, T, 1]

aux_mask = tf.expand_dims(mask, 2) # [B, T, 1]

stat_fea = tf.placeholder(tf.float32, [None, STAT_FEATURE_NUM]) # [B, FEA_NUM]

hist_loan_seq = tf.placeholder(tf.float32, [None, LOAN_SEQ_LENGTH, LOAN_FEATURE_NUM]) # [B, T, FEA_NUM]

hist_repay_seq = tf.placeholder(tf.float32, [None, REPAY_SEQ_LENGTH, REPAY_FEATURE_NUM]) # [B, T, FEA_NUM]

hist_chain_seq = tf.placeholder(tf.float32, [None, CHAIN_SEQ_LENGTH, CHAIN_FEATURE_NUM])



with tf.name_scope('loan_seq'):
    loan_hiddens, loan_states = single_layer_dynamic_gru(hist_loan_seq, LOAN_SEQ_LENGTH, LOAN_FEATURE_NUM, 'loan_seq_gru')

with tf.name_scope('repay_seq'):
    repay_hiddens, repay_states = single_layer_dynamic_gru(hist_repay_seq, REPAY_SEQ_LENGTH, REPAY_FEATURE_NUM, 'repay_seq_gru')
    
with tf.name_scope('chain_seq'):
    chain_hiddens, chain_states = single_layer_dynamic_gru(hist_chain_seq, CHAIN_SEQ_LENGTH, CHAIN_FEATURE_NUM, 'chain_seq_gru')
    
with tf.name_scope('stat_feature'):
    o1 = tf.layers.dense(stat_fea, 128, activation=None, name='stat_fc_1')
    o2 = tf.nn.relu(o1)
    o3 = tf.layers.dense(o2, 64, activation=None, name='stat_fc_2')
    stat_hiddens = tf.nn.relu(o3)

with tf.name_scope('account_seq_and_stat_for_dien_module'):
    # RNN for history sequence
    with tf.name_scope('hist_account_seq_encoder'):
        rnn_outputs, _ = dynamic_rnn(GRUCell(EVENT_FEATURE_NUM),  # set hidden size = EVENT_FEATURE_NUM
                                     inputs=hist_account_seq,
                                     sequence_length=hist_account_seq_length,
                                     dtype=tf.float32,
                                     scope="account_gru_hist")

    #     tf.summary.histogram('GRU_output', rnn_outputs)

    with tf.name_scope('auxiliary_net_loss'):
        aux_loss = auxiliary_loss(rnn_outputs, hist_click_label, aux_mask)


    # Attention layer
    with tf.name_scope('Attention_layer'):
        att_outputs, alphas = din_fcn_attention(target_account, 
                                                rnn_outputs, 
                                                EVENT_FEATURE_NUM, # set hidden size = EVENT_FEATURE_NUM
                                                mask=mask,
                                                softmax_stag=1, 
                                                stag='1_1', 
                                                mode='LIST', 
                                                return_alphas=True)

    #     tf.summary.histogram('alpha_output', alphas)

    # RNN for top history sequence
    with tf.name_scope('rnn_top'):
        rnn_outputs2, final_state2 = dynamic_rnn(VecAttGRUCell(EVENT_FEATURE_NUM), # set hidden size = EVENT_FEATURE_NUM
                                                 inputs=rnn_outputs,
                                                 att_scores =tf.expand_dims(alphas, -1),
                                                 sequence_length=hist_account_seq_length,
                                                 dtype=tf.float32,
                                                 scope="vec_att_gru")

with tf.name_scope('combine_layer'):
    combine_layer = tf.concat([loan_hiddens[-1], repay_hiddens[-1], chain_hiddens[-1], stat_hiddens, final_state2, target_account], axis=1, name='combine_op')

with tf.name_scope('all_fc'):
    a1 = tf.layers.dense(combine_layer, 128, activation=None, name='all_fc_1')
    a2 = tf.nn.relu(a1)
    a3 = tf.layers.dense(a2, 1, activation=None, name='all_output')
    pred = tf.nn.sigmoid(a3)
    
    

print(loan_hiddens[-1], repay_hiddens[-1], chain_hiddens[-1], stat_hiddens, final_state2, target_account)

params = [param for param in tf.get_collection(tf.GraphKeys.GLOBAL_VARIABLES)]
for item in params:
    print(item)

y_true = tf.placeholder(tf.float32, [None, 1])
loss_train = tf.reduce_mean(-tf.reduce_sum(1.0*y_true*tf.log(pred) + (1-y_true)*tf.log(1-pred), reduction_indices=1))
loss = loss_train + aux_loss

train_op = tf.train.AdamOptimizer(ETA).minimize(loss, var_list = params) 

print('Load data...')
# --------------------------------------------- load data ---------------------------------------------
data_dir = '/nfs/project/wuyifan_data/user_loan_demand/'

train_info_path = data_dir + 'feature_model_file/train_info.csv'
test1_info_path = data_dir + 'feature_model_file/test1_info.csv'
test_info_path = data_dir + 'feature_model_file/test_info.csv'

train_label_path = data_dir + 'feature_model_file/train_label.npy'
test1_label_path = data_dir + 'feature_model_file/test1_label.npy'
test_label_path = data_dir + 'feature_model_file/test_label.npy'

train_loan_seq_matrix_path = data_dir + 'feature_model_file/train_loan_seq_matrix.npy'
train_repay_seq_matrix_path = data_dir + 'feature_model_file/train_repay_seq_matrix.npy'
train_event1_seq_matrix_path = data_dir + 'feature_model_file/train_event1_seq_matrix.npy'
train_event2_seq_matrix_path = data_dir + 'feature_model_file/train_event2_seq_matrix.npy'

test1_loan_seq_matrix_path = data_dir + 'feature_model_file/test1_loan_seq_matrix.npy'
test1_repay_seq_matrix_path = data_dir + 'feature_model_file/test1_repay_seq_matrix.npy'
test1_event1_seq_matrix_path = data_dir + 'feature_model_file/test1_event1_seq_matrix.npy'
test1_event2_seq_matrix_path = data_dir + 'feature_model_file/test1_event2_seq_matrix.npy'

test_loan_seq_matrix_path = data_dir + 'feature_model_file/test_loan_seq_matrix.npy'
test_repay_seq_matrix_path = data_dir + 'feature_model_file/test_repay_seq_matrix.npy'
test_event1_seq_matrix_path = data_dir + 'feature_model_file/test_event1_seq_matrix.npy'
test_event2_seq_matrix_path = data_dir + 'feature_model_file/test_event2_seq_matrix.npy'

stat_feature_train_norm_path = data_dir + 'feature_model_file/stat_feature_train.npy'
stat_feature_test1_norm_path = data_dir + 'feature_model_file/stat_feature_test1.npy'
stat_feature_test_norm_path = data_dir + 'feature_model_file/stat_feature_test.npy'

train_account_path = data_dir + 'feature_model_file/account_feature_train.npy'
test_account_path = data_dir + 'feature_model_file/account_feature_test.npy'
test1_account_path = data_dir + 'feature_model_file/account_feature_test1.npy'

train_event_seq_matrix = np.load(train_event2_seq_matrix_path)
train_loan_seq_matrix = np.load(train_loan_seq_matrix_path)
train_repay_seq_matrix = np.load(train_repay_seq_matrix_path)

test_event_seq_matrix = np.load(test_event2_seq_matrix_path)
test_loan_seq_matrix = np.load(test_loan_seq_matrix_path)
test_repay_seq_matrix = np.load(test_repay_seq_matrix_path)

test1_event_seq_matrix = np.load(test1_event2_seq_matrix_path)
test1_loan_seq_matrix = np.load(test1_loan_seq_matrix_path)
test1_repay_seq_matrix = np.load(test1_repay_seq_matrix_path)

baseline_feature_top_train_norm = np.load(stat_feature_train_norm_path)
baseline_feature_top_test_norm = np.load(stat_feature_test_norm_path)
baseline_feature_top_test1_norm = np.load(stat_feature_test1_norm_path)

train_account = np.load(train_account_path)
test_account = np.load(test_account_path)
test1_account = np.load(test1_account_path)

train_label = np.load(train_label_path)
test_label = np.load(test_label_path)
test1_label = np.load(test1_label_path)

# accout seq for dien model
train_account_seq_matrix = train_event_seq_matrix[:, :, 10:]
test_account_seq_matrix = test_event_seq_matrix[:, :, 10:]
test1_account_seq_matrix = test1_event_seq_matrix[:, :, 10:]

# aux label for dien model 
aux_label_train = pd.DataFrame(train_event_seq_matrix[:, :, 6].reshape((-1,1)), columns=['aux_label'])
aux_label_test = pd.DataFrame(test_event_seq_matrix[:, :, 6].reshape((-1,1)), columns=['aux_label'])
aux_label_test1 = pd.DataFrame(test1_event_seq_matrix[:, :, 6].reshape((-1,1)), columns=['aux_label'])

aux_label_train['aux_label_01'] = aux_label_train['aux_label'].apply(lambda x:1 if x>0 else 0)
aux_label_test['aux_label_01'] = aux_label_test['aux_label'].apply(lambda x:1 if x>0 else 0)
aux_label_test1['aux_label_01'] = aux_label_test1['aux_label'].apply(lambda x:1 if x>0 else 0)

train_account_seq_aux_label = np.array(aux_label_train['aux_label_01']).reshape((-1, 20))
test_account_seq_aux_label = np.array(aux_label_test['aux_label_01']).reshape((-1, 20))
test1_account_seq_aux_label = np.array(aux_label_test1['aux_label_01']).reshape((-1, 20))


hist_account_seq_length_train_data = np.array([EVENT_SEQ_LENGTH]*train_label.shape[0]).reshape((-1,)) #[B]
hist_account_seq_length_test_data = np.array([EVENT_SEQ_LENGTH]*test_label.shape[0]).reshape((-1,)) #[B]
hist_account_seq_length_test1_data = np.array([EVENT_SEQ_LENGTH]*test1_label.shape[0]).reshape((-1,)) #[B]

mask_train_data = np.array([1]*train_label.shape[0]*EVENT_SEQ_LENGTH).reshape((-1,EVENT_SEQ_LENGTH)) # [B, T]
mask_test_data = np.array([1]*test_label.shape[0]*EVENT_SEQ_LENGTH).reshape((-1,EVENT_SEQ_LENGTH)) # [B, T]
mask_test1_data = np.array([1]*test1_label.shape[0]*EVENT_SEQ_LENGTH).reshape((-1,EVENT_SEQ_LENGTH)) # [B, T]


sess = tf.Session()
sess.run(tf.global_variables_initializer()) #初始化变量 
        
#  -------------------------- Train --------------------------
for step in range(EPOCHES):
    i=0
    for train_data in  get_batch(BATCH_SIZE, 
                                    baseline_feature_top_train_norm,
                                    train_loan_seq_matrix,
                                    train_repay_seq_matrix,
                                    train_event_seq_matrix[:, :, :10],
                                    train_account_seq_matrix, 
                                    hist_account_seq_length_train_data, 
                                    train_account, 
                                    mask_train_data,
                                    train_account_seq_aux_label.reshape((-1,20,1)),
                                    train_label.reshape((-1,1)), 
                                    train_label.shape[0]):
    
        _ = sess.run([train_op], {
                                    stat_fea:train_data[0],
                                    hist_loan_seq:train_data[1],
                                    hist_repay_seq:train_data[2],
                                    hist_chain_seq:train_data[3],
                                    hist_account_seq: train_data[4],
                                    hist_account_seq_length: train_data[5],
                                    target_account: train_data[6],
                                    mask: train_data[7],
                                    hist_click_label: train_data[8],
                                    y_true: train_data[9]
                                   })


        
        if i%10==0:
#             loss1_train, loss2_train, pred_train= sess.run([loss_train, aux_loss, pred], 
#                                       {
#                                         stat_fea: baseline_feature_top_train_norm,
#                                         hist_loan_seq: train_loan_seq_matrix,
#                                         hist_repay_seq: train_repay_seq_matrix,
#                                         hist_chain_seq: train_event_seq_matrix[:, :, :10],
#                                         hist_account_seq: train_account_seq_matrix,
#                                         hist_account_seq_length: hist_account_seq_length_train_data,
#                                         target_account: train_account,
#                                         mask: mask_train_data,
#                                         hist_click_label: train_account_seq_aux_label.reshape((-1,20,1)),
#                                         y_true: train_label.reshape((-1,1))
#                                        })
#             train_auc = auc(pred_train, train_label.reshape((-1,1)))
            
            loss1, loss2, pred_= sess.run([loss_train, aux_loss, pred], 
                                      {
                                        stat_fea: baseline_feature_top_test_norm,
                                        hist_loan_seq: test_loan_seq_matrix,
                                        hist_repay_seq: test_repay_seq_matrix,
                                        hist_chain_seq: test_event_seq_matrix[:, :, :10],
                                        hist_account_seq: test_account_seq_matrix,
                                        hist_account_seq_length: hist_account_seq_length_test_data,
                                        target_account: test_account,
                                        mask: mask_test_data,
                                        hist_click_label: test_account_seq_aux_label.reshape((-1,20,1)),
                                        y_true: test_label.reshape((-1,1))
                                       })
            
            test_auc = auc(pred_, test_label.reshape((-1,1)))
#             print('step:{} - batch:{} ==> loss_train:{}, loss_aux:{}, test_auc:{} loss_train:{}, loss_aux:{}, test_auc:{}'.format(step, i, loss1_train, loss2_train, train_auc, loss1, loss2, test_auc))
            print('step:{} - batch:{} ==> loss_train:{}, loss_aux:{}, test_auc:{}'.format(step, i, loss1, loss2, test_auc))

        i+=1
