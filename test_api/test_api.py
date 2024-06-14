import urllib.request
import json
from pathlib import Path
import psycopg2

ROOT_DIR = Path(__file__).parent
DB_USER = "postgres"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

# Em caso de querer buscar todos os 1048576 do URL_DISTRIBUIDORA, basta aumentar o limit para isto
URL_GERACAO_DISTRIBUIDORA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=b1bd71e7-d0ad-4214-9053-cbd58e9564a7&limit=5000"
URL_INFO_TEC_EOLICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=5f903d78-25ae-4a3f-a2bd-9a93351c59fb"
URL_INFO_TEC_FOTOVOLTAICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a&limit=5000"
URL_INFO_TEC_HIDRELETRICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=c189442a-18f0-44eb-9c89-3b48147a4d65"
URL_INFO_TEC_TERMELETRICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=bd1d3783-b389-49d8-a828-a56e193d0671&limit=581"

def convertValue(value):
    try:
        converted_value = float(value.replace(",", "."))
        return converted_value
    except ValueError:
        return value

def convertNumeric(value):
    if value == '' or value is None:
        return None
    else:
        return float(value.replace(",", "."))


def fetchData(url):
    response = urllib.request.urlopen(url)
    data = response.read()
    json_data = json.loads(data)
    return json_data["result"]["records"]

def empreendimento_geracao_distribuida():
    table_name = "Empreendimento_Geracao_Distribuida"
    records = fetchData(URL_GERACAO_DISTRIBUIDORA)

    connection = psycopg2.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            DatGeracaoConjuntoDados DATE,
            AnmPeriodoReferencia VARCHAR(7),
            NumCNPJDistribuidora VARCHAR(14),
            SigAgente VARCHAR(20),
            NomAgente VARCHAR(200),
            CodClasseConsumo INT,
            DscClasseConsumo VARCHAR(20),
            CodSubGrupoTarifario INT,
            DscSubGrupoTarifario VARCHAR(10),
            CodUFibge VARCHAR(2),
            SigUF VARCHAR(2),
            CodRegiao VARCHAR(4),
            NomRegiao VARCHAR(50),
            CodMunicipioIbge INT,
            NomMunicipio VARCHAR(40),
            SigTipoConsumidor VARCHAR(2),
            NumCPFCNPJ VARCHAR(14),
            NomTitularEmpreendimento VARCHAR(8000),
            CodEmpreendimento VARCHAR(21),
            DthAtualizaCadastralEmpreend TIMESTAMP,
            SigModalidadeEmpreendimento VARCHAR(1),
            DscModalidadeHabilitado VARCHAR(50),
            QtdUCRecebeCredito NUMERIC(16,2),
            SigTipoGeracao VARCHAR(10),
            DscFonteGeracao VARCHAR(50),
            DscPorte VARCHAR(12),
            NumCoordNEmpreendimento VARCHAR(30),
            NumCoordEEmpreendimento VARCHAR(30),
            MdaPotenciaInstaladaKW NUMERIC(6,2),
            NomSubEstacao VARCHAR(50),
            NumCoordESub VARCHAR(30),
            NumCoordNSub VARCHAR(30)
        )
        """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            AnmPeriodoReferencia,
            NumCNPJDistribuidora,
            SigAgente,
            NomAgente,
            CodClasseConsumo,
            DscClasseConsumo,
            CodSubGrupoTarifario,
            DscSubGrupoTarifario,
            CodUFibge,
            SigUF,
            CodRegiao,
            NomRegiao,
            CodMunicipioIbge,
            NomMunicipio,
            SigTipoConsumidor,
            NumCPFCNPJ,
            NomTitularEmpreendimento,
            CodEmpreendimento,
            DthAtualizaCadastralEmpreend,
            SigModalidadeEmpreendimento,
            DscModalidadeHabilitado,
            QtdUCRecebeCredito,
            SigTipoGeracao,
            DscFonteGeracao,
            DscPorte,
            NumCoordNEmpreendimento,
            NumCoordEEmpreendimento,
            MdaPotenciaInstaladaKW,
            NomSubEstacao,
            NumCoordESub,
            NumCoordNSub
        ) VALUES (
            %(DatGeracaoConjuntoDados)s,
            %(AnmPeriodoReferencia)s,
            %(NumCNPJDistribuidora)s,
            %(SigAgente)s,
            %(NomAgente)s,
            %(CodClasseConsumo)s,
            %(DscClasseConsumo)s,
            %(CodSubGrupoTarifario)s,
            %(DscSubGrupoTarifario)s,
            %(CodUFibge)s,
            %(SigUF)s,
            %(CodRegiao)s,
            %(NomRegiao)s,
            %(CodMunicipioIbge)s,
            %(NomMunicipio)s,
            %(SigTipoConsumidor)s,
            %(NumCPFCNPJ)s,
            %(NomTitularEmpreendimento)s,
            %(CodEmpreendimento)s,
            %(DthAtualizaCadastralEmpreend)s,
            %(SigModalidadeEmpreendimento)s,
            %(DscModalidadeHabilitado)s,
            %(QtdUCRecebeCredito)s,
            %(SigTipoGeracao)s,
            %(DscFonteGeracao)s,
            %(DscPorte)s,
            %(NumCoordNEmpreendimento)s,
            %(NumCoordEEmpreendimento)s,
            %(MdaPotenciaInstaladaKW)s,
            %(NomSubEstacao)s,
            %(NumCoordESub)s,
            %(NumCoordNSub)s
        )
    """

    for record in records:
        record["MdaPotenciaInstaladaKW"] = convertValue(record["MdaPotenciaInstaladaKW"])
        cursor.execute(sql, record)

    connection.commit()
    cursor.close()
    connection.close()

def infoTecEolica():
    records = fetchData(URL_INFO_TEC_EOLICA)
    table_name = "InfoTecEolica"

    connection = psycopg2.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            DatGeracaoConjuntoDados DATE,
            CodGeracaoDistribuida VARCHAR(21),
            NomFabricanteAerogerador VARCHAR(100),
            DscModeloAerogerador VARCHAR(100),
            MdaPotenciaInstalada NUMERIC(6,2),
            MdaAlturaPa NUMERIC(5,2),
            DatConexao DATE,
            IdcEixoRotor VARCHAR(1)
        )
        """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            NomFabricanteAerogerador,
            DscModeloAerogerador,
            MdaPotenciaInstalada,
            MdaAlturaPa,
            DatConexao,
            IdcEixoRotor
        ) VALUES (
            %(DatGeracaoConjuntoDados)s,
            %(CodGeracaoDistribuida)s,
            %(NomFabricanteAerogerador)s,
            %(DscModeloAerogerador)s,
            %(MdaPotenciaInstalada)s,
            %(MdaAlturaPa)s,
            %(DatConexao)s,
            %(IdcEixoRotor)s
        )
    """

    for record in records:
        record["MdaPotenciaInstalada"] = convertValue(record.get("MdaPotenciaInstalada"))
        record["MdaAlturaPa"] = convertValue(record.get("MdaAlturaPa"))
        cursor.execute(sql, record)

    connection.commit()
    cursor.close()
    connection.close()

def infoTecFotovoltaica():
    records = fetchData(URL_INFO_TEC_FOTOVOLTAICA)
    table_name = "InfoTecFotovoltaica"

    connection = psycopg2.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            DatGeracaoConjuntoDados DATE,
            CodGeracaoDistribuida VARCHAR(21),
            MdaAreaArranjo NUMERIC(8,2),
            MdaPotenciaInstalada NUMERIC(6,2),
            NomFabricanteModulo VARCHAR(100),
            NomFabricanteInversor VARCHAR(100),
            DatConexao DATE,
            MdaPotenciaModulos NUMERIC(6,2),
            MdaPotenciaInversores NUMERIC(6,2),
            QtdModulos INT,
            NomModeloModulo VARCHAR(100),
            NomModeloInversor VARCHAR(100)
        )
        """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            MdaAreaArranjo,
            MdaPotenciaInstalada,
            NomFabricanteModulo,
            NomFabricanteInversor,
            DatConexao,
            MdaPotenciaModulos,
            MdaPotenciaInversores,
            QtdModulos,
            NomModeloModulo,
            NomModeloInversor
        ) VALUES (
            %(DatGeracaoConjuntoDados)s,
            %(CodGeracaoDistribuida)s,
            %(MdaAreaArranjo)s,
            %(MdaPotenciaInstalada)s,
            %(NomFabricanteModulo)s,
            %(NomFabricanteInversor)s,
            %(DatConexao)s,
            %(MdaPotenciaModulos)s,
            %(MdaPotenciaInversores)s,
            %(QtdModulos)s,
            %(NomModeloModulo)s,
            %(NomModeloInversor)s
        )
    """

    for record in records:
        record["MdaAreaArranjo"] = convertValue(record.get("MdaAreaArranjo"))
        record["MdaPotenciaInstalada"] = convertValue(record.get("MdaPotenciaInstalada"))
        record["MdaPotenciaModulos"] = convertValue(record.get("MdaPotenciaModulos"))
        record["MdaPotenciaInversores"] = convertValue(record.get("MdaPotenciaInversores"))
        cursor.execute(sql, record)

    connection.commit()
    cursor.close()
    connection.close()

def infoTecHidreletrica():
    table_name = "InfoTecHidreletrica"
    records = fetchData(URL_INFO_TEC_HIDRELETRICA)

    connection = psycopg2.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            DatGeracaoConjuntoDados DATE,
            CodGeracaoDistribuida VARCHAR(21),
            NomRio VARCHAR(50),
            MdaPotenciaInstalada NUMERIC(6,2),
            DatConexao DATE,
            MdaPotenciaAparente NUMERIC(6,2),
            MdaFatorPotencia NUMERIC(6,2),
            MdaTensao NUMERIC(6,2),
            MdaNivelOperacionalMontante NUMERIC(6,2),
            MdaNivelOperacionalJusante NUMERIC(6,2)
        )
        """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            NomRio,
            MdaPotenciaInstalada,
            DatConexao,
            MdaPotenciaAparente,
            MdaFatorPotencia,
            MdaTensao,
            MdaNivelOperacionalMontante,
            MdaNivelOperacionalJusante
        ) VALUES (
            %(DatGeracaoConjuntoDados)s,
            %(CodGeracaoDistribuida)s,
            %(NomRio)s,
            %(MdaPotenciaInstalada)s,
            %(DatConexao)s,
            %(MdaPotenciaAparente)s,
            %(MdaFatorPotencia)s,
            %(MdaTensao)s,
            %(MdaNivelOperacionalMontante)s,
            %(MdaNivelOperacionalJusante)s
        )
    """

    for record in records:
        record["MdaPotenciaInstalada"] = convertNumeric(record.get("MdaPotenciaInstalada"))
        record["MdaPotenciaAparente"] = convertNumeric(record.get("MdaPotenciaAparente"))
        record["MdaFatorPotencia"] = convertNumeric(record.get("MdaFatorPotencia"))
        record["MdaTensao"] = convertNumeric(record.get("MdaTensao"))
        record["MdaNivelOperacionalMontante"] = convertNumeric(record.get("MdaNivelOperacionalMontante"))
        record["MdaNivelOperacionalJusante"] = convertNumeric(record.get("MdaNivelOperacionalJusante"))

        cursor.execute(sql, record)

    connection.commit()
    cursor.close()
    connection.close()

def infoTecTermeletrica():
    records = fetchData(URL_INFO_TEC_TERMELETRICA)
    table_name = "InfoTermeletrica"

    connection = psycopg2.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            DatGeracaoConjuntoDados DATE,
            CodGeracaoDistribuida VARCHAR(21),
            MdaPotenciaInstalada NUMERIC(6,2),
            DatConexao DATE,
            DscCicloTermodinamico VARCHAR(30),
            DscMaquinaMotrizTermica VARCHAR(30)
        )
        """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            MdaPotenciaInstalada,
            DatConexao,
            DscCicloTermodinamico,
            DscMaquinaMotrizTermica
        ) VALUES (
            %(DatGeracaoConjuntoDados)s,
            %(CodGeracaoDistribuida)s,
            %(MdaPotenciaInstalada)s,
            %(DatConexao)s,
            %(DscCicloTermodinamico)s,
            %(DscMaquinaMotrizTermica)s
        )
    """

    for record in records:
        record["MdaPotenciaInstalada"] = convertValue(record.get("MdaPotenciaInstalada"))
        cursor.execute(sql, record)

    connection.commit()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    empreendimento_geracao_distribuida()
    infoTecEolica()
    infoTecFotovoltaica()
    infoTecHidreletrica()
    infoTecTermeletrica()
