import streamlit as st
import pandas as pd
import re
import sqlite3
import os
import calendar
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DB_PATH = "dados_conselho_fiscal.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS dados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referencia TEXT,
            tipo TEXT,
            grupo TEXT,
            item TEXT,
            competencia TEXT,
            liquidacao TEXT,
            documento TEXT,
            forma_pgto TEXT,
            valor REAL
        )
    """)
    conn.commit()
    conn.close()

def inserir_dados(df, referencia):
    conn = sqlite3.connect(DB_PATH)
    df = df.copy()
    df['referencia'] = referencia
    df = df[['referencia', 'Tipo', 'Grupo', 'Item', 'Competência', 'Liquidação', 'Documento', 'Forma de Pgto.', 'Valor']]
    df.columns = ['referencia', 'tipo', 'grupo', 'item', 'competencia', 'liquidacao', 'documento', 'forma_pgto', 'valor']
    df.to_sql('dados', conn, if_exists='append', index=False)
    conn.close()

def carregar_referencias():
    conn = sqlite3.connect(DB_PATH)
    refs = pd.read_sql_query("SELECT DISTINCT referencia FROM dados ORDER BY referencia DESC", conn)
    conn.close()
    return refs['referencia'].tolist()

def carregar_dados_por_referencia(referencia):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM dados WHERE referencia = ?", conn, params=(referencia,))
    conn.close()
    return df

def excluir_referencia(referencia):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM dados WHERE referencia = ?", (referencia,))
    conn.commit()
    conn.close()

def excluir_todos():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM dados")
    conn.commit()
    conn.close()

def extrair_referencia_padronizada(df):
    """Extrai e padroniza a referência para o formato mm/aaaa"""
    referencia = df['Liquidação'].mode(dropna=True)
    if not referencia.empty:
        ref_raw = str(referencia.iloc[0])
        
        # Tenta converter para mm/aaaa
        match = re.match(r"(\d{1,2})/(\d{4})", ref_raw)
        if match:
            mes = int(match.group(1))
            ano = match.group(2)
            return f"{mes:02d}/{ano}"
        
        # Tenta converter de formato textual (ex: jan/2025)
        meses = {v.lower(): f"{k:02d}" for k,v in enumerate(calendar.month_abbr) if v}
        partes = ref_raw.lower().split('/')
        if len(partes) == 2 and partes[0] in meses:
            return f"{meses[partes[0]]}/{partes[1]}"
        
        # Fallback para formato original
        return ref_raw
    return 'Desconhecido'

def formatar_valor_brasileiro(valor):
    """Formata valores para padrão brasileiro (R$ 1.234,56)"""
    if pd.isna(valor):
        return ""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# Inicializa o banco
init_db()

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

# Botão para excluir todos os dados (mais discreto)
with st.expander("⚠️ Configurações Avançadas"):
    if st.button("Excluir TODOS os dados do banco de dados", type="secondary"):
        excluir_todos()
        st.success("Todos os dados foram excluídos.")

aba_analise, aba_historico = st.tabs(["Análise do Mês", "Histórico de Meses"])

with aba_analise:
    uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx)", type=["xlsx"], key="uploader")

    if uploaded_file is not None:
        # Mostrar progresso de forma discreta
        with st.spinner("Processando arquivo..."):
            df_processed = process_excel_file(uploaded_file)

        if df_processed is not None:
            # Extrai e padroniza a referência
            referencia_str = extrair_referencia_padronizada(df_processed)
            
            # Verifica se já existe referência no banco
            referencias_existentes = carregar_referencias()
            
            if referencia_str in referencias_existentes:
                st.error(f"⚠️ Referência **{referencia_str}** já foi importada. Exclua o período anterior antes de importar novamente.")
            else:
                inserir_dados(df_processed, referencia_str)
                # Status de importação discreto
                st.success(f"✅ Período **{referencia_str}** importado com sucesso!")
            
            # Exibir período de referência carregado
            st.info(f"📅 **Período de referência carregado:** {referencia_str}")

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
              label="📥 Baixar Dados Processados em Excel",
              data=f.read(),
              file_name=output_filename,
              mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            st.subheader("📊 Informações do Arquivo:")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total de linhas", df_processed.shape[0])
            with col2:
                st.metric("Total de colunas", df_processed.shape[1])

            # Menu para selecionar entre Receita e Despesa
            menu_opcao = st.radio("Selecione o tipo para visualizar o resumo:", ("Receita", "Despesa"))

            menu_resumo = st.radio(
                "Selecione o tipo de resumo de Despesas:",
                ("Grupo", "Forma de Pagamento")
            )

            if menu_opcao == "Receita":
                st.subheader("💰 Resumo de Receitas por Grupo")
                resumo_receita = df_processed[df_processed['Tipo'] == 'Receita'].groupby('Grupo')['Valor'].sum().reset_index()
                resumo_receita = resumo_receita.sort_values(by='Valor', ascending=False)
                total_receita = resumo_receita['Valor'].sum()
                resumo_receita['% do Total'] = resumo_receita['Valor'] / total_receita * 100
                resumo_receita['Valor'] = resumo_receita['Valor'].apply(formatar_valor_brasileiro)
                resumo_receita['% do Total'] = resumo_receita['% do Total'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(resumo_receita, use_container_width=True)
            else:
                if menu_resumo == "Grupo":
                    st.subheader("💸 Resumo de Despesas por Grupo")
                    resumo_despesa = df_processed[df_processed['Tipo'] == 'Despesa'].groupby('Grupo')['Valor'].sum().reset_index()
                    resumo_despesa = resumo_despesa.sort_values(by='Valor', ascending=False)
                    total_despesa = resumo_despesa['Valor'].sum()
                    resumo_despesa['% do Total'] = resumo_despesa['Valor'] / total_despesa * 100
                    resumo_despesa['Valor'] = resumo_despesa['Valor'].apply(formatar_valor_brasileiro)
                    resumo_despesa['% do Total'] = resumo_despesa['% do Total'].apply(lambda x: f"{x:.2f}%")
                    st.dataframe(resumo_despesa, use_container_width=True)
                else:
                    st.subheader("💳 Resumo de Despesas por Forma de Pagamento")
                    resumo_fp = (
                        df_processed[df_processed['Tipo'] == 'Despesa']
                        .groupby('Forma de Pgto.')['Valor']
                        .sum()
                        .reset_index()
                        .sort_values(by='Valor', ascending=False)
                    )
                    total_despesa_fp = resumo_fp['Valor'].sum()
                    resumo_fp['% do Total'] = resumo_fp['Valor'] / total_despesa_fp * 100
                    resumo_fp['Valor'] = resumo_fp['Valor'].apply(formatar_valor_brasileiro)
                    resumo_fp['% do Total'] = resumo_fp['% do Total'].apply(lambda x: f"{x:.2f}%")
                    st.dataframe(resumo_fp, use_container_width=True)

            st.subheader("📈 Resumo Total")
            total_summary = df_processed.groupby('Tipo')['Valor'].sum().reset_index()

            # Salve os totais numéricos ANTES de formatar para string
            total_receitas = total_summary[total_summary['Tipo'] == 'Receita']['Valor'].sum()
            total_despesas = total_summary[total_summary['Tipo'] == 'Despesa']['Valor'].sum()
            saldo = total_receitas - total_despesas

            # Formatar valores para exibição
            total_summary['Valor'] = total_summary['Valor'].apply(formatar_valor_brasileiro)
            st.dataframe(total_summary, use_container_width=True)

            # Destacar saldo com cor
            saldo_color = "green" if saldo >= 0 else "red"
            st.markdown(f"### <span style='color: {saldo_color}'>💰 Saldo Total (Receitas - Despesas): {formatar_valor_brasileiro(saldo)}</span>", unsafe_allow_html=True)

with aba_historico:
    referencias = carregar_referencias()
    if referencias:
        st.subheader("📅 Histórico de Períodos Importados")
        
        # Ordena as referências no formato mm/aaaa corretamente
        def referencia_key(ref):
            match = re.match(r"(\d{2})/(\d{4})", ref)
            if match:
                return int(match.group(2)) * 100 + int(match.group(1))
            return 0
        
        referencias_ordenadas = sorted(referencias, key=referencia_key, reverse=True)
        
        for ref in referencias_ordenadas:
            with st.expander(f"📊 Período: {ref}", expanded=False):
                col1, col2 = st.columns([8, 2])
                
                with col2:
                    if st.button(f"🗑️ Excluir", key=f"del_{ref}", type="secondary"):
                        excluir_referencia(ref)
                        st.success(f"Período {ref} excluído com sucesso!")
                        st.rerun()
                
                with col1:
                    df_hist = carregar_dados_por_referencia(ref)
                    
                    # Métricas principais
                    total_receitas_hist = df_hist[df_hist['tipo'] == 'Receita']['valor'].sum()
                    total_despesas_hist = df_hist[df_hist['tipo'] == 'Despesa']['valor'].sum()
                    saldo_hist = total_receitas_hist - total_despesas_hist
                    
                    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                    with col_m1:
                        st.metric("📊 Total de Registros", df_hist.shape[0])
                    with col_m2:
                        st.metric("💰 Total Receitas", formatar_valor_brasileiro(total_receitas_hist))
                    with col_m3:
                        st.metric("💸 Total Despesas", formatar_valor_brasileiro(total_despesas_hist))
                    with col_m4:
                        delta_color = "normal" if saldo_hist >= 0 else "inverse"
                        st.metric("⚖️ Saldo", formatar_valor_brasileiro(saldo_hist), delta_color=delta_color)
                    
                    # Gráfico de comparação Receitas vs Despesas
                    resumo_tipos = df_hist.groupby('tipo')['valor'].sum().reset_index()
                    if not resumo_tipos.empty:
                        fig_tipos = px.bar(
                            resumo_tipos, 
                            x='tipo', 
                            y='valor',
                            title=f"Receitas vs Despesas - {ref}",
                            color='tipo',
                            color_discrete_map={'Receita': '#2E8B57', 'Despesa': '#DC143C'}
                        )
                        fig_tipos.update_layout(showlegend=False, height=400)
                        fig_tipos.update_traces(texttemplate='%{y:,.0f}', textposition='outside')
                        st.plotly_chart(fig_tipos, use_container_width=True)
                    
                    # Gráficos lado a lado para grupos
                    col_g1, col_g2 = st.columns(2)
                    
                    # Gráfico de despesas por grupo
                    with col_g1:
                        despesas_hist = df_hist[df_hist['tipo'] == 'Despesa']
                        if not despesas_hist.empty:
                            grupo_desp = despesas_hist.groupby('grupo')['valor'].sum().sort_values(ascending=False).head(10)
                            fig_desp = px.pie(
                                values=grupo_desp.values,
                                names=grupo_desp.index,
                                title="Top 10 Despesas por Grupo"
                            )
                            fig_desp.update_layout(height=400)
                            st.plotly_chart(fig_desp, use_container_width=True)
                    
                    # Gráfico de receitas por grupo
                    with col_g2:
                        receitas_hist = df_hist[df_hist['tipo'] == 'Receita']
                        if not receitas_hist.empty:
                            grupo_rec = receitas_hist.groupby('grupo')['valor'].sum().sort_values(ascending=True).head(10)
                            fig_rec = px.bar(
                                x=grupo_rec.values,
                                y=grupo_rec.index,
                                orientation='h',
                                title="Top 10 Receitas por Grupo",
                                color_discrete_sequence=['#2E8B57']
                            )
                            fig_rec.update_layout(
                                height=400,
                                xaxis_title="Valor (R$)",
                                yaxis_title="Grupo"
                            )
                            fig_rec.update_traces(texttemplate='%{x:,.0f}', textposition='outside')
                            st.plotly_chart(fig_rec, use_container_width=True)
                    
                    # Tabela detalhada de formas de pagamento (apenas para despesas)
                    if not despesas_hist.empty:
                        st.subheader("💳 Despesas por Forma de Pagamento")
                        forma_pgto = despesas_hist.groupby('forma_pgto')['valor'].sum().sort_values(ascending=False)
                        df_forma_pgto = forma_pgto.reset_index()
                        df_forma_pgto['valor_formatado'] = df_forma_pgto['valor'].apply(formatar_valor_brasileiro)
                        df_forma_pgto['percentual'] = (df_forma_pgto['valor'] / df_forma_pgto['valor'].sum() * 100).apply(lambda x: f"{x:.1f}%")
                        
                        # Exibir tabela formatada
                        st.dataframe(
                            df_forma_pgto[['forma_pgto', 'valor_formatado', 'percentual']].rename(columns={
                                'forma_pgto': 'Forma de Pagamento',
                                'valor_formatado': 'Valor',
                                'percentual': 'Percentual'
                            }),
                            use_container_width=True
                        )
    else:
        st.info("📋 Nenhum período importado ainda. Carregue um arquivo na aba 'Análise do Mês' para começar.")