# 🦟 Monitoramento Epidemiológico: Dengue 2024 (SINAN)
**Projeto Final - Fundamentos de Engenharia de Dados (PECE Poli-USP)**

**Responsável:** Alan Cristian Oliveira Freire da Silva  
**Domínio:** Saúde Pública

---

## 📖 Storytelling e Perguntas de Negócio
Em 2024, o Brasil enfrentou um dos maiores surtos de Dengue da história. Este pipeline ELT processa dados brutos do SINAN (Sistema de Informação de Agravos de Notificação) para transformar registros administrativos em insights de saúde pública.

**Perguntas que o Dashboard responde:**
1. Qual a curva de evolução de casos notificados ao longo dos meses de 2024?
2. Qual a distribuição de casos por sexo (Feminino, Masculino e Ignorado)?
3. A qualidade dos dados brutos permite uma análise confiável (integridade de campos críticos)?

---

## 🛠️ Stack Tecnológica
* **Orquestração:** Apache Airflow 2.7.1
* **Data Quality:** Great Expectations (GX)
* **Transformação:** dbt (Data Build Tool)
* **Visualização:** Grafana
* **Banco de Dados:** PostgreSQL 15
* **Infraestrutura:** Docker & Docker Compose

---

## 🏗️ Arquitetura de Dados (Medalhão)
1.  **Raw:** Ingestão do CSV bruto, validação de contrato com GX e persistência no Postgres.
2.  **Silver:** Limpeza de tipos, conversão da idade (código SINAN para anos reais) e normalização.
3.  **Gold:** Agregações mensais e por demografia para consumo do Grafana.

---

## 🚀 Como Executar
1. **Clone o repositório:**
   ```bash
   git clone [https://github.com/alancristians/ProjetoFinal-FEDUSP.git](https://github.com/alancristians/ProjetoFinal-FEDUSP.git)