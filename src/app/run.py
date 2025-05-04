import sys
sys.path.append('../')

import asyncio

from create_app import create_app
from retrieval import IngredientAtlasSearch
from database import DBClient


async def main():
    app = await create_app()
    app.run(host = "0.0.0.0", port = 8000, debug = True)

if __name__ == "__main__":
    asyncio.run(main())