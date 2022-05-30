'''

Author : Vipul Patel
Description : Create app and called scrapy file

'''

import pandas as pd
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import uvicorn
from io import BytesIO
from scrapy.cmdline import execute

app = FastAPI()


@app.post("/cdw")
async def cdw(category: str, sub_category: str):
    sub = sub_category.replace(' ', '_')
    try:
        execute(f"scrapy runspider DataExtractor.py -a category={category} -a subcategory={sub} --nolog".split())
    except:
        pass
    frame = pd.read_excel("temp.xlsx")
    output = BytesIO()

    with pd.ExcelWriter(output) as writer:
        frame.to_excel(writer)
    headers = {
        'Content-Disposition': f'attachment; filename={category}_{sub_category}.xlsx'
    }
    return StreamingResponse(iter([output.getvalue()]), headers=headers)


if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8004)
    print("running")
