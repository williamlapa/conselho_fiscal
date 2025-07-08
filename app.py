import streamlit as st
import pandas as pd
import re

def clean_and_convert_value(value):
    """
    Cleans and converts a string value to a float, handling parentheses for negative numbers,
    percentage signs, thousands separators, and decimal commas.
    """
    value_str = str(value).strip()
    is_negative = False

    if value_str.startswith('(') and value_str.endswith(')'):
        is_negative = True
        value_str = value_str[1:-1] # Remove parentheses

    value_str = value_str.replace('%', '') # Remove percentage sign
    value_str = value_str.replace('.', '') # Remove thousands separator (dot)
    value_str = value_str.replace(',', '.') # Replace comma with dot for decimal

    try:
        numeric_value = float(value_str)
        if is_negative:
            numeric_value *= -1
        return numeric_value
    except ValueError:
        return pd.NA # Return Not Available for values that cannot be converted

def process_excel_file(uploaded_file):
    """
    Processes the uploaded Excel file to extract and combine
    revenue and expense data into a standardized DataFrame.
    """
    df = pd.read_excel(uploaded_file)

    # Find header rows for 'Receitas' and 'Despesas'
    linhas_cabecalho_receitas = df[df.iloc[:, 0].astype(str).str.strip() == 'Receitas']
    linhas_cabecalho_despesas = df[df.iloc[:, 0].astype(str).str.strip() == 'Despesas']

    indice_linha_cabecalho_receitas = -1
    if not linhas_cabecalho_receitas.empty:
        indice_linha_cabecalho_receitas = linhas_cabecalho_receitas.index[0]

    indice_linha_cabecalho_despesas = -1
    if not linhas_cabecalho_despesas.empty:
        linhas_cabecalho_despesas_apos_receitas = linhas_cabecalho_despesas[linhas_cabecalho_despesas.index > indice_linha_cabecalho_receitas]
        if not linhas_cabecalho_despesas_apos_receitas.empty:
            indice_linha_cabecalho_despesas = linhas_cabecalho_despesas_apos_receitas.index[0]

    if indice_linha_cabecalho_receitas == -1 or indice_linha_cabecalho_despesas == -1:
        st.error("Não foi possível encontrar os cabeçalhos 'Receitas' ou 'Despesas' com correspondência exata. Verifique o conteúdo do arquivo.")
        return None

    # --- Process Receitas ---
    df_dados_brutos_receitas = df.iloc[indice_linha_cabecalho_receitas + 1 : indice_linha_cabecalho_despesas].copy()
    df_receitas = pd.DataFrame()
    df_receitas['Item'] = df_dados_brutos_receitas.iloc[:, 0]
    df_receitas['Competência'] = df_dados_brutos_receitas.iloc[:, 1]
    df_receitas['Liquidação'] = df_dados_brutos_receitas.iloc[:, 2]
    df_receitas['Valor'] = df_dados_brutos_receitas.iloc[:, 4]
    df_receitas['Grupo_Checker'] = df_dados_brutos_receitas.iloc[:, 5]
    df_receitas['Tipo'] = 'Receita'

    df_receitas = df_receitas[~df_receitas['Item'].astype(str).str.startswith('Total', na=False)].copy()

    df_receitas['Grupo'] = ''
    grupo_atual_receita = ''
    for idx in df_receitas.index:
        eh_cabecalho_grupo = (pd.isna(df_receitas.loc[idx, 'Grupo_Checker']) or str(df_receitas.loc[idx, 'Grupo_Checker']).strip() == '') and \
                             (pd.isna(df_receitas.loc[idx, 'Valor']) or str(df_receitas.loc[idx, 'Valor']).strip() == '')
        if eh_cabecalho_grupo:
            grupo_atual_receita = df_receitas.loc[idx, 'Item']
        else:
            df_receitas.loc[idx, 'Grupo'] = grupo_atual_receita

    df_receitas_processadas = df_receitas[
        ~((pd.isna(df_receitas['Grupo_Checker']) | (df_receitas['Grupo_Checker'].astype(str).str.strip() == '')) &\
          (pd.isna(df_receitas['Valor']) | (df_receitas['Valor'].astype(str).str.strip() == '')))].copy()


    # --- Process Despesas ---
    df_dados_brutos_despesas = df.iloc[indice_linha_cabecalho_despesas + 1:].copy()
    df_despesas = pd.DataFrame()
    df_despesas['Item'] = df_dados_brutos_despesas.iloc[:, 0]
    df_despesas['Competência'] = df_dados_brutos_despesas.iloc[:, 1]
    df_despesas['Liquidação'] = df_dados_brutos_despesas.iloc[:, 2]
    df_despesas['Documento'] = df_dados_brutos_despesas.iloc[:, 3]
    df_despesas['Forma de Pgto.'] = df_dados_brutos_despesas.iloc[:, 4]
    df_despesas['Grupo_Checker'] = df_dados_brutos_despesas.iloc[:, 5]
    df_despesas['Valor'] = df_dados_brutos_despesas.iloc[:, 6]
    df_despesas['Tipo'] = 'Despesa'

    df_despesas = df_despesas[~df_despesas['Item'].astype(str).str.startswith('Total', na=False)].copy()

    df_despesas['Grupo'] = ''
    grupo_atual_despesa = ''
    for i in range(len(df_despesas)):
        if pd.isna(df_despesas.iloc[i]['Grupo_Checker']) or str(df_despesas.iloc[i]['Grupo_Checker']).strip() == '':
            grupo_atual_despesa = df_despesas.iloc[i]['Item']
        else:
            df_despesas.loc[df_despesas.index[i], 'Grupo'] = grupo_atual_despesa

    df_despesas_processadas = df_despesas.dropna(subset=['Grupo_Checker']).copy()

    # --- Standardize and Combine DataFrames ---
    colunas_finais = ['Tipo', 'Grupo', 'Item', 'Competência', 'Liquidação', 'Documento', 'Forma de Pgto.', 'Valor']

    for col in ['Documento', 'Forma de Pgto.']:
        if col not in df_receitas_processadas.columns:
            df_receitas_processadas[col] = None
        df_receitas_processadas[col] = df_receitas_processadas[col].astype(object)

    df_receitas_processadas_final = df_receitas_processadas.reindex(columns=colunas_finais)
    df_despesas_processadas_final = df_despesas_processadas.reindex(columns=colunas_finais)

    df_final = pd.concat([df_receitas_processadas_final, df_despesas_processadas_final], ignore_index=True)

    df_final['Valor'] = df_final['Valor'].apply(clean_and_convert_value)
    df_final['Valor'] = pd.to_numeric(df_final['Valor'], errors='coerce')

    df_final['Forma de Pgto.'] = df_final['Forma de Pgto.'].replace('', pd.NA)
    df_final['Forma de Pgto.'] = df_final['Forma de Pgto.'].fillna('Outros')

    return df_final

# Streamlit App
st.set_page_config(page_title="Conversor Receitas Despesas Analítico", layout="wide")

st.title("Conversor Receitas Despesas Analítico")

st.markdown("""
Esta ferramenta converte o seu arquivo Excel de receitas e despesas em um formato analítico padronizado.
Por favor, carregue o arquivo Excel contendo as seções 'Receitas' e 'Despesas'.
""")

uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx)", type=["xlsx"])

if uploaded_file is not None:
    st.info("Processando o arquivo, por favor aguarde...")
    df_processed = process_excel_file(uploaded_file)

    if df_processed is not None:
        st.success("Arquivo processado com sucesso!")

        # st.subheader("Dados Processados")
        # st.dataframe(df_processed)

        # Generate download button for the processed Excel file
        competencia_mais_frequente = df_processed['Competência'].mode(dropna=True)
        if not competencia_mais_frequente.empty:
            competencia_str = competencia_mais_frequente.iloc[0].replace('/', '_')
            output_filename = f'receitas_despesas_{competencia_str}.xlsx'
        else:
            output_filename = 'receitas_despesas.xlsx'

        output_excel_buffer = pd.ExcelWriter('temp.xlsx', engine='xlsxwriter')
        df_processed.to_excel(output_excel_buffer, index=False)
        output_excel_buffer.close()

        # Custom CSS to change the download button color
        st.markdown("""
            <style>
            .stDownloadButton>button {
          background-color: #4CAF50 !important;
          color: white !important;
            }
            </style>
        """, unsafe_allow_html=True)

        with open('temp.xlsx', 'rb') as f:
            st.download_button(
          label="Baixar Dados Processados em Excel",
          data=f.read(),
          file_name=output_filename,
          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        st.subheader("Informações do Arquivo final:")
        st.write(f"Número total de linhas: {df_processed.shape[0]}")
        st.write(f"Número total de colunas: {df_processed.shape[1]}")        

        # Menu para selecionar entre Receita e Despesa
        menu_opcao = st.radio("Selecione o tipo para visualizar o resumo:", ("Receita", "Despesa"))

        def formatar_valor(valor):
            if pd.isna(valor):
                return ""
            return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        menu_resumo = st.radio(
            "Selecione o tipo de resumo de Despesas:",
            ("Grupo", "Forma de Pagamento")
        )

        if menu_opcao == "Receita":
            st.subheader("Resumo de Receitas por Grupo")
            resumo_receita = df_processed[df_processed['Tipo'] == 'Receita'].groupby('Grupo')['Valor'].sum().reset_index()
            resumo_receita = resumo_receita.sort_values(by='Valor', ascending=False)
            total_receita = resumo_receita['Valor'].sum()
            resumo_receita['% do Total'] = resumo_receita['Valor'] / total_receita * 100
            resumo_receita['Valor'] = resumo_receita['Valor'].apply(formatar_valor)
            resumo_receita['% do Total'] = resumo_receita['% do Total'].apply(lambda x: f"{x:.2f}%")
            st.dataframe(resumo_receita)
        else:
            if menu_resumo == "Grupo":
                st.subheader("Resumo de Despesas por Grupo")
                resumo_despesa = df_processed[df_processed['Tipo'] == 'Despesa'].groupby('Grupo')['Valor'].sum().reset_index()
                resumo_despesa = resumo_despesa.sort_values(by='Valor', ascending=False)
                total_despesa = resumo_despesa['Valor'].sum()
                resumo_despesa['% do Total'] = resumo_despesa['Valor'] / total_despesa * 100
                resumo_despesa['Valor'] = resumo_despesa['Valor'].apply(formatar_valor)
                resumo_despesa['% do Total'] = resumo_despesa['% do Total'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(resumo_despesa)
            else:
                st.subheader("Resumo de Despesas por Forma de Pagamento")
                resumo_fp = (
                    df_processed[df_processed['Tipo'] == 'Despesa']
                    .groupby('Forma de Pgto.')['Valor']
                    .sum()
                    .reset_index()
                    .sort_values(by='Valor', ascending=False)
                )
                total_despesa_fp = resumo_fp['Valor'].sum()
                resumo_fp['% do Total'] = resumo_fp['Valor'] / total_despesa_fp * 100
                resumo_fp['Valor'] = resumo_fp['Valor'].apply(formatar_valor)
                resumo_fp['% do Total'] = resumo_fp['% do Total'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(resumo_fp)

        st.subheader("Resumo Total")
        total_summary = df_processed.groupby('Tipo')['Valor'].sum().reset_index()

        # Salve os totais numéricos ANTES de formatar para string
        total_receitas = total_summary[total_summary['Tipo'] == 'Receita']['Valor'].sum()
        total_despesas = total_summary[total_summary['Tipo'] == 'Despesa']['Valor'].sum()
        saldo = total_receitas - total_despesas

        # Formatar valores para padrão brasileiro (R$ 1.234,56)
        def formatar_valor_br(valor):
            if pd.isna(valor):
                return ""
            return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        total_summary['Valor'] = total_summary['Valor'].apply(formatar_valor_br)
        st.dataframe(total_summary)

        st.markdown(f"**Saldo Total (Receitas - Despesas): R$ {saldo:,.2f}**")


else:
    st.info("Por favor, carregue um arquivo Excel para começar.")