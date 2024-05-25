index="random"
dataset="Electronics"
backbone="roberta-base"  # Change to the desired RoBERTa model
bs=128
sample=0.2
path='../../data/'
epoch=2
lr=1e-3
wd=0.01
valid=0
model_dir='../../models'

CUDA_VISIBLE_DEVICES=0 CUDA_LAUNCH_BLOCKING=1 torchrun --nproc_per_node=8 --master_port=1247 ../src/src_roberta/train.py --data_path ${path} --model_dir ${model_dir} --item_indexing ${index} --tasks sequential,straightforward --datasets ${dataset} --epochs ${epoch} --lr ${lr} --batch_size ${bs} --weight_decay ${wd} --backbone ${backbone} --lora 1 --task_alternating_optim 1 --sample_ratio ${sample} --valid_select ${valid} > ../log/${dataset}/${dataset}_roberta_${index}_valid${valid}_lr${lr}_wd${wd}_${epoch}epoch_${bs}bs_sample${sample}.log