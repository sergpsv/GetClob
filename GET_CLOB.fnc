CREATE OR REPLACE FUNCTION GET_CLOB(p_sql varchar2,
                  p_inclide_Header      boolean  default true, 
                  p_delimiter           varchar2 default ';',
                  nls_codepage          varchar2 default null, 
                  p_remove_bad_scv_char boolean  default true,
                  p_enclose_quotes      number   default 0
                  ) return clob
is
   -- author = sparshukov
   -- goal   = получение в CLOB результата запроса в формате CSV
   
   -- nls_codepage := 'CL8MSWIN1251'
   l_rowCounter  number   := 0;
   l_theCursor   number   := dbms_sql.open_cursor;
   l_colCnt      number   :=0;            -- кол-во колонок
   l_status      number   :=0;            -- результат выполнения запроса
   l_colValue    varchar2(1500)  :='';     -- значение столбца
   l_descTbl     dbms_sql.desc_tab;       -- таблица описаний
   l_delimiter   varchar2(10)   := p_delimiter;
   reportClob    clob;

   buff_size     number := 32696;  
   l_buff        varchar2(32696) :='';     -- строка результата
   l_line        varchar2(5000)  :='';     -- строка результата
   l_col_name    varchar2(50) := '';
   l_ech         varchar2(1)  := '';
begin
  dbms_lob.createtemporary(reportClob,false);
  l_rowCounter := 0;
  l_ech := case when p_enclose_quotes=1 then '"' else '' end;
  
  -- анализируем запрос
  begin
    dbms_sql.parse(l_theCursor, p_sql, dbms_sql.native);
  exception
    when others then
      log_ovart(-1, 'get_clob', p_sql);
      raise;
  end;
  -- получаем описание результатов запроса
  dbms_sql.describe_columns(l_theCursor, l_colCnt, l_descTbl);
  -- формируем заголовок
  for i in 1..l_colCnt
  loop
    if nls_codepage is not null then
      l_col_name := convert(l_descTbl(i).col_name,nls_codepage);
    else
      l_col_name := l_descTbl(i).col_name;
    end if;
    if i=1 then
      l_line := l_col_name;
    else
      l_line := l_line ||l_delimiter|| l_col_name;
    end if;
    dbms_sql.define_column(l_theCursor, i, l_colValue, 500);
  end loop;
  if p_inclide_Header then 
    l_line := l_line || chr(13)||chr(10);
    dbms_lob.writeappend(reportClob, length(l_line),l_line);  
  end if;

  -- выполняем запрос
  l_status := dbms_sql.execute(l_theCursor);
  
  -- извлекаем результаты
  l_buff := ''; 
  while (dbms_sql.fetch_rows(l_theCursor) > 0 )
  loop
    l_line := '';
    for i in 1..l_colCnt
    loop
      dbms_sql.column_value(l_theCursor, i, l_colValue);
      if p_remove_bad_scv_char then 
        l_colValue := l_ech||translate(l_colValue,';'''||chr(13)||chr(10)||chr(9)||p_delimiter,':`    ')||l_ech;
      end if;
      if i=1 then
        l_line := l_colValue;
      else
        l_line := l_line || l_delimiter || l_colValue;
      end if;
    end loop;
    l_rowCounter := l_rowCounter +1;
    if nls_codepage is not null then 
      l_line := convert(l_line||chr(13)||chr(10),nls_codepage);
    else
      l_line := l_line||chr(13)||chr(10);
    end if;
    if (length(l_buff)+ length(l_line)) > buff_size then
      dbms_lob.writeappend(reportClob, length(l_buff),l_buff);  
--      log_ovart(1,'get_clob','l_rowCounter='||to_char(l_rowCounter)||' length(l_buff)='||to_char(length(l_buff))); 
      l_buff := l_line;
    else    
      l_buff := l_buff || l_line;
    end if;
  end loop;
  if length(l_buff)>0 then
      dbms_lob.writeappend(reportClob, length(l_buff),l_buff);  
--      log_ovart(0,'get_clob','l_rowCounter='||to_char(l_rowCounter)); 
      l_buff := '';
  end if;

  dbms_sql.close_cursor(l_theCursor);
--  commit;

  return reportClob;

end;
/
