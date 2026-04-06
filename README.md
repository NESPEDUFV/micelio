# Micelio: **MI**ddleware for **C**ontext r**E**asoning through federated **L**earning in the **IO**T computing continuum

## Simulação

Para executar uma simulação, siga os seguintes passos:

- Defina a variável de ambiente `NS3_HOME` para apontar para a sua pasta raiz do [ns3](https://www.nsnam.org/).
- Se estiver usando o VS COde, adicione `"${env:NS3_HOME}/build/include"` ao arquivo `.vscode/c_cpp_properties.json`, no caminho `configurations[].includePath[]`. Exemplo:
```
{
    "configurations": [
        {
            "includePath": [
                "${workspaceFolder}/**",
                "${env:NS3_HOME}/build/include"
            ]
        }
    ]
}
```
- Adicione um arquivo chamado `.env` contendo as seguintes variáveis:
```
SIM_NAME=micelio
SIM_PROFILE=debug
BUILD_PROFILE=debug
NS3_HOME= 
JENA_FUSEKI_HOME= # caminho para pasta contendo o Jena preparado para uso com Docker
JENA_FUSEKI_IMAGE=jena-fuseki-5.2
SIM_PARAMS=data/simulation/trash-40-case1.ttl # arquivo de parâmetros para a simulação
LIBTORCH= # caminho para pasta contendo a biblioteca torch 
RESNET_PATH= # caminho para o modelo pré-treinado headless da resnet18
MICELIO_ML_DIRECTORY= # caminho para uma pasta onde modelos treinados podem ser salvos
```
- (opcional) Obtenha o [conjunto de dados](https://huggingface.co/datasets/garythung/trashnet) e formate-o com o script `./scripts/prepare-data.py`.
- Execute `make run`.


