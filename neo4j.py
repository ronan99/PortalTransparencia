# -*- coding: utf-8 -*-


import zipfile
import os
import pandas as pd
import sys
global_path = os.getcwd()
sys.path.append('../')
path_to_file = '/zip/DadosPortalTransparencia.zip'
pathToCsv = global_path + '/csvFiles/'
import shutil

from py2neo import Graph

password = input('digite a senha do banco')
graph = Graph(password = password)






dict = {
  "201908_CNPJ.csv": "cnpj",
  "201908_Cadastro.csv": "cadastro",
  "201908_Compras.csv": "compras",
  "201908_Licitaá∆o.csv": "licitacao",
  "201908_ItemLicitaá∆o.csv": "itemLicitacao",
  "201908_ParticipantesLicitaá∆o.csv": "partLicitacao",
  "201908_TermoAditivo.csv": "termoAditivo",
  "201908_Socios.csv": "socios",
  
}


def start():
  if(not os.path.exists(pathToCsv)):
    os.mkdir(pathToCsv)
    with zipfile.ZipFile(global_path+path_to_file, 'r') as zip_ref:
      zip_ref.extractall( pathToCsv)
    shutil.rmtree(pathToCsv + '__MACOSX')
  
def readCsv():
  chunk_list = []
  data = []
  for csv in os.listdir(pathToCsv):
    
    df_chunk = (pd.read_csv(pathToCsv + csv , sep = ';',encoding = 'cp860',low_memory = False,chunksize=10000))
    for chunk in df_chunk:  

        
        # Once the data filtering is done, append the chunk to list
        chunk_list.append(chunk)
        
    # concat the list into dataframe x
    data.append( pd.concat(chunk_list,sort=False))
    chunk_list = []
    print(csv)
  return data
  

  


def neo4jPrepare():
  
  data = readCsv()
  i = 0

  for index,lines in data[0].iterrows():
    graph.run('CREATE (:Pessoa { nome:"'+lines['NOME'] + '",CPF: "' + lines['CPF']  + '" } )')
    if(i == 15):
      i=0
      break
    i= i+ 1
  
  for index,lines in data[1].iterrows():
    graph.run('CREATE (:Empresa { nome_social:$nomesocial ,nome_fantasia:$nome_fantasia ,CNPJ:$cnpj } )',nomesocial=str(lines['RAZAOSOCIAL']),nome_fantasia= str(lines['NOMEFANTASIA']),cnpj=str(lines['CNPJ']))
    if(i == 15):
      i=0
      break
    i= i+ 1

  for index,lines in data[6].iterrows():
    if( not isinstance(lines['CPF-CNPJ'],float)):
      if(len(lines['CPF-CNPJ']) >14):

        graph.run('MATCH (p:Empresa { CNPJ:$cnpj }), (m:Empresa {CNPJ: $cnpjEmpresa}) CREATE (p)-[:SOCIO {tipo:[$tipo]}]->(m) ',cnpj = str(lines['CPF-CNPJ']),cnpjEmpresa = str(lines['CNPJ']), tipo =str(lines['Tipo']))
      else:
        graph.run('MATCH (p:Pessoa { CPF: $cpfCnpj }), (m:Empresa {CNPJ: $cnpj }) CREATE (p)-[:SOCIO {tipo:[$tipo]}]->(m) ', cpfCnpj =str(lines['CPF-CNPJ']),cnpj = str(lines['CNPJ']),tipo = str(lines['Tipo'])) 
    if(i == 15):
      i=0
      break
    i= i+ 1


  for index,lines in data[4].iterrows():
    graph.run('CREATE (:Licitacao { numero:$numero ,objeto:$objeto ,situacao:$situacao,valor:$valor, data: $data } )',numero=str(lines['N·mero Licitaτπo']),objeto=str(lines['Objeto']),situacao=str(lines['Situaτπo Licitaτπo']),valor=str(lines['Valor Licitaτπo']),data=str(lines['Data Resultado Compra'])   )
    if(i == 15):
      i=0
      break
    i= i+ 1

  
  for index,lines in data[2].iterrows():
    graph.run('CREATE (:Contrato { numero: $numero,objeto:$objeto ,valor:$valor ,data:$data } )', numero =str(lines['N·mero do Contrato']),objeto=str(lines['Objeto']),valor=str(lines['Valor Final Compra']),data=str(lines['Data Publicaτπo DOU']) )
    graph.run('MATCH (p:Orgao { codigo:$codigo }), (m:Contrato { numero:$numero }) CREATE (p)-[:CONTRATOU ]->(m) ',codigo = str(lines['C≤digo ╙rgπo']),numero =str(lines['N·mero do Contrato']))
    if(i == 15):
      i=0
      break
    i= i+ 1
  aux = [] 
  test = []
  for index,lines in data[4].iterrows():
    if(not lines['C≤digo ╙rgπo'] in test):
      aux.append([lines['C≤digo ╙rgπo'] , lines['Nome ╙rgπo']])
      test.append(lines['C≤digo ╙rgπo'])


  for line in aux:
    graph.run('CREATE (:Orgao { nome:$nome, codigo:$codigo })',nome=str(line[1]),codigo=str(line[0]))
    if(i == 15):
      i=0
      break
    i= i+ 1

  for index,lines in data[0].iterrows():
    graph.run('MATCH (p:Pessoa { CPF:$cpf }), (m:Orgao {codigo:$codigo }) CREATE (p)-[:SERVIDOR {matricula:[$matricula],cargo:[$cargo],situacao:[$situacao] }]->(m) ', cpf=str(lines['CPF']),codigo=str(lines['COD_ORG_LOTACAO']),matricula=str(lines['MATRICULA']),cargo=str(lines['DESCRICAO_CARGO']),situacao=str(lines['SITUACAO_VINCULO'])  )
    if(i == 15):
      i=0
      break
    i= i+ 1

  for index,lines in data[5].iterrows():
    graph.run('MATCH (p:Orgao { codigo:$codigo }), (m:Licitacao {numero:$numero}) CREATE (p)-[:LICITOU ]->(m) ',codigo=str(lines['C≤digo ╙rgπo']),numero=str(lines['N·mero Licitaτπo']))
    graph.run('MATCH (p:Empresa { CNPJ:$cnpj}), (m:Licitacao {numero:$numero}) CREATE (p)-[:PARTICIPOU ]->(m) ',cnpj=str(lines['CNPJ Participante']),numero=str(lines['N·mero Licitaτπo']))
    if(i == 15):
      i=0
      break
    i= i+ 1

  for index,lines in data[2].iterrows():
    graph.run('MATCH (p:Empresa { CNPJ:$cnpj}), (m:Contrato {numero: $numero}) CREATE (p)-[:ATUOU ]->(m) ',cnpj=str(lines['CNPJ Contratado']),numero = str(lines['N·mero do Contrato']))
    if(i == 15):
      i=0
      break
    i= i+ 1

#servidor,socio,licitou,contratou,atuou todos os nós.
  

def main():

  start()
  neo4jPrepare()



  


# driver = new4jDriver('bolt://localhost:7687',"neo4j", "123456")


if __name__ == "__main__":
  main()