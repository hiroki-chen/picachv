for project:
  - policy_eval ==> non-agg: policy_eval
  - process     ==> non-agg: process

for aggregate:
  - group by        ==> aggregate: groupby
  - policy_eval_gb  ==> groupby: policy_eval
  - policy_eval_agg ==> aggregate: policy_eval
  - process         ==> aggregate: process